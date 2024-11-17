from typing import List
from datetime import datetime, timedelta

from prefect import flow, get_run_logger, task
from prefect.client import get_client
from prefect.variables import Variable
from sqlalchemy import and_
from sqlalchemy.orm import Session

from punchpipe.control.db import Flow
from punchpipe.control.util import get_database_session, load_pipeline_configuration


@task
def gather_planned_flows(session):
    return [f.flow_id for f in session.query(Flow).where(Flow.state == "planned").order_by(Flow.priority.desc()).all()]


@task
def count_running_flows(session):
    return len(session.query(Flow).where(Flow.state == "running").all())


@task
def escalate_long_waiting_flows(session, pipeline_config):
    for flow_type in pipeline_config["levels"]:
        for max_seconds_waiting, escalated_priority in zip(
            pipeline_config["levels"][flow_type]["priority"]["seconds"],
            pipeline_config["levels"][flow_type]["priority"]["escalation"],
        ):
            since = datetime.now() - timedelta(seconds=max_seconds_waiting)
            session.query(Flow).where(
                and_(Flow.state == "planned", Flow.creation_time < since, Flow.flow_type == flow_type)
            ).update({"priority": escalated_priority})
            session.commit()


@task
def filter_for_launchable_flows(planned_flows, running_flow_count, max_flows_running):
    logger = get_run_logger()

    number_to_launch = max_flows_running - running_flow_count
    logger.info(f"{number_to_launch} flows can be launched at this time.")

    if number_to_launch > 0:
        if planned_flows:  # there are flows to run
            return planned_flows[:number_to_launch]
        else:
            return []
    else:
        return []


@task
async def launch_ready_flows(session: Session, flow_ids: List[int]) -> List:
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
    # gather the flow information for launching
    flow_info = session.query(Flow).where(Flow.flow_id.in_(flow_ids)).all()

    responses = []
    async with get_client() as client:
        # determine the deployment ids for each kind of flow
        deployments = await client.read_deployments()
        deployment_ids = {d.name: d.id for d in deployments}

        # for every flow launch it and store the response
        for this_flow in flow_info:
            this_deployment_id = deployment_ids[this_flow.flow_type]
            response = await client.create_flow_run_from_deployment(
                this_deployment_id, parameters={"flow_id": this_flow.flow_id}
            )
            responses.append(response)
    return responses


@flow
async def launcher_flow(pipeline_configuration_path=None):
    """The main launcher flow for Prefect, responsible for identifying flows, based on priority,
        that are ready to run and creating flow runs for them. It also escalates long-waiting flows' priorities.

    See EM 41 or the internal requirements document for more details

    Returns
    -------
    Nothing
    """
    logger = get_run_logger()

    if pipeline_configuration_path is None:
        pipeline_configuration_path = await Variable.get("punchpipe_config", "punchpipe_config.yaml")
    pipeline_config = load_pipeline_configuration(pipeline_configuration_path)

    logger.info("Establishing database connection")
    session = get_database_session()

    # Perform the launcher flow responsibilities
    num_running_flows = count_running_flows(session)
    logger.info(f"There are {num_running_flows} flows running right now.")
    escalate_long_waiting_flows(session, pipeline_config)
    queued_flows = gather_planned_flows(session)
    logger.info(f"There are {len(queued_flows)} planned flows right now.")
    flows_to_launch = filter_for_launchable_flows(
        queued_flows, num_running_flows, pipeline_config["launcher"]["max_flows_running"]
    )
    logger.info(f"Flows with IDs of {flows_to_launch} will be launched.")
    await launch_ready_flows(session, flows_to_launch)
    logger.info("Launcher flow exit.")
