import asyncio
from math import ceil
from typing import List
from datetime import datetime, timedelta

from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from prefect.client import get_client
from prefect.variables import Variable
from sqlalchemy import and_, func, select, update
from sqlalchemy.orm import Session

from punchpipe.control.db import Flow
from punchpipe.control.util import batched, get_database_session, load_pipeline_configuration


@task(cache_policy=NO_CACHE)
def gather_planned_flows(session, max_to_select=9e9):
    return [f.flow_id for f in session.query(Flow)
                                      .where(Flow.state == "planned")
                                      .order_by(Flow.priority.desc())
                                      .limit(max_to_select).all()]


@task(cache_policy=NO_CACHE)
def count_flows(session):
    n_planned, n_running = 0, 0
    rows = session.execute(
        select(Flow.state, func.count())
        .select_from(Flow)
        .where(Flow.state.in_(("planned", "running")))
        .group_by(Flow.state)
    ).all()
    # We won't get results for states that aren't actually in the database, so we have to inspect the returned rows
    for state, count in rows:
        if state == "planned":
            n_planned = count
        else:
            n_running = count
    return n_running, n_planned


@task(cache_policy=NO_CACHE)
def escalate_long_waiting_flows(session, pipeline_config):
    for flow_type in pipeline_config["flows"]:
        for max_seconds_waiting, escalated_priority in zip(
            pipeline_config["flows"][flow_type]["priority"]["seconds"],
            pipeline_config["flows"][flow_type]["priority"]["escalation"],
        ):
            since = datetime.now() - timedelta(seconds=max_seconds_waiting)
            session.query(Flow).where(
                and_(Flow.priority < escalated_priority,
                     Flow.state == "planned",
                     Flow.creation_time < since,
                     Flow.flow_type == flow_type)
            ).update({"priority": escalated_priority})
    session.commit()


def determine_launchable_flow_count(n_planned, n_running, max_running, max_to_launch):
    logger = get_run_logger()
    number_to_launch = max_running - n_running
    logger.info(f"{number_to_launch} flows can be launched at this time.")

    number_to_launch = min(number_to_launch, max_to_launch)
    number_to_launch = max(0, number_to_launch)
    logger.info(f"Will launch up to {number_to_launch} flows")

    return min(number_to_launch, n_planned)


@task(cache_policy=NO_CACHE)
async def launch_ready_flows(session: Session, flow_ids: List[int], pipeline_config: dict) -> None:
    """Given a list of ready-to-launch flow_ids, this task creates flow runs in Prefect for them.
    These flow runs are automatically marked as scheduled in Prefect and will be picked up by a work queue and
    agent as soon as possible.

    Parameters
    ----------
    session : sqlalchemy.orm.session.Session
        A SQLAlchemy session for database interactions
    flow_ids : List[int]
        A list of flow IDs from the punchpipe database identifying which flows to launch

    Returns
    -------
    A list of responses from Prefect about the flow runs that were created
    """
    if not len(flow_ids):
        return
    logger = get_run_logger()
    # gather the flow information for launching
    flow_info = session.query(Flow).where(Flow.flow_id.in_(flow_ids)).all()

    async with get_client() as client:
        # determine the deployment ids for each kind of flow
        deployments = await client.read_deployments()
        deployment_ids = {d.name: d.id for d in deployments}

        for flow in flow_info:
            flow.state = "launched"
            flow.launch_time = datetime.now()
        session.commit()

        # We want to stagger launches through a time window. If our configured time window is 5 minutes, we'll use 4
        # full minutes, plus a portion of the fifth, aiming to end after 4m35s to leave margin so the flow is fully
        # finished after 5 minutes.
        # First we work out the remainder part, figuring out where we are relative to the 35th second of the current
        # minute.
        total_delay_time = 35 - datetime.now().second
        total_delay_time = max(0, total_delay_time)
        total_delay_time += (pipeline_config['control']['launcher']['launch_time_window_minutes'] - 1) * 60
        # Launch a batch every 10 seconds through this window
        n_batches = total_delay_time // 10
        batch_size = ceil(len(flow_info) / n_batches)
        logger.info(f"Total delay time: {total_delay_time}")
        if batch_size >= len(flow_info):
            delay_time = 0
        else:
            delay_time = total_delay_time / (n_batches - 1)
        awaitables = []
        responses = []
        for batch in batched(flow_info, batch_size):
            start = datetime.now().timestamp()
            # Launch the batch
            for this_flow in batch:
                this_deployment_id = deployment_ids[this_flow.flow_type + "_process_flow"]
                awaitables.append(client.create_flow_run_from_deployment(
                    this_deployment_id, parameters={"flow_id": this_flow.flow_id})
                )

            responses.extend(await asyncio.gather(*awaitables))
            awaitables = []
            logger.info(f"Batch of {len(batch)} sent")
            if delay_time:
                # Stagger the launches
                await asyncio.sleep(delay_time - (datetime.now().timestamp() - start))

        # TODO This doesn't seem to be an effective way to check for a failed flow submission, but we should
        # do something like this that works
        ok_responses = [r for r in responses if r.name not in [None, ''] and r.state_name == 'Scheduled']
        bad_responses = [r for r in responses if r not in ok_responses]

        if len(bad_responses):
            session.execute(
                update(Flow)
                .where(Flow.state == 'launched')
                .where(Flow.flow_id.in_([r.parameters['flow_id'] for r in bad_responses]))
                .values(state='planned')
            )
            session.commit()
            for r in bad_responses:
                logger.warning(f"Got bad response {repr(r)}")


@flow
async def launcher(pipeline_config_path=None):
    """The main launcher flow for Prefect, responsible for identifying flows, based on priority,
        that are ready to run and creating flow runs for them. It also escalates long-waiting flows' priorities.

    See EM 41 or the internal requirements document for more details

    Returns
    -------
    Nothing
    """
    logger = get_run_logger()

    if pipeline_config_path is None:
        pipeline_config_path = await Variable.get("punchpipe_config", "punchpipe_config.yaml")
    pipeline_config = load_pipeline_configuration(pipeline_config_path)

    logger.info("Establishing database connection")
    session = get_database_session()

    escalate_long_waiting_flows(session, pipeline_config)

    # Perform the launcher flow responsibilities
    num_running_flows, num_planned_flows = count_flows(session)
    logger.info(f"There are {num_running_flows} flows running right now and {num_planned_flows} planned flows.")
    max_flows_running = pipeline_config["control"]["launcher"]["max_flows_running"]
    max_flows_to_launch = pipeline_config["control"]["launcher"]["max_flows_to_launch_at_once"]

    number_to_launch = determine_launchable_flow_count(
        num_planned_flows, num_running_flows, max_flows_running, max_flows_to_launch)

    flows_to_launch = gather_planned_flows(session, number_to_launch)
    logger.info(f"{len(flows_to_launch)} flows with IDs of {flows_to_launch} will be launched.")
    await launch_ready_flows(session, flows_to_launch, pipeline_config)
    logger.info("Launcher flow exit.")
