import os
import asyncio
from pathlib import Path
from datetime import datetime, timedelta

from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterStartTime,
    FlowRunFilterState,
    FlowRunFilterStateType,
)
from prefect.client.schemas.objects import StateType
from sqlalchemy.orm import aliased

from punchpipe.control.db import File, FileRelationship, Flow
from punchpipe.control.util import get_database_session, load_pipeline_configuration


@flow
def cleaner(pipeline_config_path: str, session=None):
    logger = get_run_logger()

    pipeline_config = load_pipeline_configuration(pipeline_config_path)
    if session is None:
        session = get_database_session()

    reset_revivable_flows(logger, session, pipeline_config)

    # because flows in the launched state aren't running in Prefect yet, we don't update them there
    fail_stuck_flows(logger, session, pipeline_config, "launched", update_prefect=False)

    # running flows are both in Prefect and in our punchpipe database, so we have to cancel them both places
    fail_stuck_flows(logger, session, pipeline_config, "running", update_prefect=True)

@task(cache_policy=NO_CACHE)
def reset_revivable_flows(logger, session, pipeline_config):
    # Note: I thought about adding a maximum here, but this flow takes only 5 seconds to revive 10,000 L1 flows, so I
    # think we're good.
    child = aliased(File)
    parent = aliased(File)
    results = (session.query(FileRelationship, parent, child, Flow)
               .join(parent, parent.file_id == FileRelationship.parent)
               .join(child, child.file_id == FileRelationship.child)
               .join(Flow, Flow.flow_id == child.processing_flow)
               .where(Flow.state == 'revivable')
              ).all()

    # This one loops differently than the others, because we need to track the child that's being deleted to know how
    # to reset the parent.
    unique_parents = set()
    for _, parent, child, processing_flow in results:
        # Handle the case that both L2 and LQ have been set to 'revivable'. If the LQ shows up first in this loop and
        # we set the L1's state to 'created', we don't want to later set it to 'quickpunched' when the L2 shows up.
        if processing_flow.flow_type != 'construct_stray_light':
            parent.state = "created"
        unique_parents.add(parent.file_id)
    logger.info(f"Reset {len(unique_parents)} parent files")

    unique_children = {child for rel, parent, child, flow in results}
    root_path = Path(pipeline_config["root"])
    for child in unique_children:
        output_path = Path(child.directory(pipeline_config["root"])) / child.filename()
        if output_path.exists():
            os.remove(output_path)
        sha_path = str(output_path) + '.sha'
        if os.path.exists(sha_path):
            os.remove(sha_path)
        jp2_path = output_path.with_suffix('.jp2')
        if jp2_path.exists():
            os.remove(jp2_path)
        # Iteratively remove parent directories if they're empty. output_path.parents gives the file's parent dir,
        # then that dir's parent, then that dir's parent...
        for parent_dir in output_path.parents:
            if not parent_dir.exists():
                break
            if len(os.listdir(parent_dir)):
                break
            if parent_dir == root_path:
                break
            parent_dir.rmdir()
        session.delete(child)
    logger.info(f"Deleted {len(unique_children)} child files")

    # Every FileRelationship item is unique
    for relationship, _, _, _ in results:
        session.delete(relationship)
    logger.info(f"Cleared {len(results)} file relationships")

    unique_flows = {flow for rel, parent, child, flow in results}
    for f in unique_flows:
        session.delete(f)
    logger.info(f"Deleted {len(unique_flows)} flows")

    session.commit()
    if len(unique_flows):
        logger.info(f"Processed {len(unique_flows)} revivable flows")


@task(cache_policy=NO_CACHE)
async def cancel_running_prefect_flows_before_cutoff(
        cutoff: datetime,
        batch_size: int = 100
):
    """Cancels flows that started running before a cutoff time."""
    logger = get_run_logger()

    async with get_client() as client:
        flow_run_filter = FlowRunFilter(
            start_time=FlowRunFilterStartTime(before_=cutoff),
            state=FlowRunFilterState(
                type=FlowRunFilterStateType(
                    any_=[StateType.RUNNING]
                )
            )
        )

        # Get flow runs to delete
        flow_runs = await client.read_flow_runs(
            flow_run_filter=flow_run_filter,
            limit=batch_size
        )

        deleted_total = 0

        while flow_runs:
            batch_deleted = 0
            failed_deletes = []

            # Delete each flow run through the API
            for flow_run in flow_runs:
                try:
                    await client.delete_flow_run(flow_run.id)
                    deleted_total += 1
                    batch_deleted += 1
                except Exception as e:
                    logger.warning(f"Failed to delete flow run {flow_run.id}: {e}")
                    failed_deletes.append(flow_run.id)

                # Rate limiting - adjust based on your API capacity
                if batch_deleted % 10 == 0:
                    await asyncio.sleep(0.5)

            logger.info(f"Deleted {batch_deleted}/{len(flow_runs)} flow runs (total: {deleted_total})")
            if failed_deletes:
                logger.warning(f"Failed to delete {len(failed_deletes)} flow runs")

            # Get next batch
            flow_runs = await client.read_flow_runs(
                flow_run_filter=flow_run_filter,
                limit=batch_size
            )

            # Delay between batches to avoid overwhelming the API
            await asyncio.sleep(1.0)

        logger.info(f"Prefect deletion complete. Total deleted: {deleted_total}")

@task(cache_policy=NO_CACHE)
def fail_stuck_flows(logger, session, pipeline_config, state, update_prefect=False):
    amount_of_patience = pipeline_config['control']['cleaner'].get(f'fail_{state}_flows_after_minutes', -1)
    if amount_of_patience < 0:
        return

    cutoff = datetime.now() - timedelta(minutes=amount_of_patience)
    stucks = (session.query(Flow)
              .where(Flow.state == state)
              .where(Flow.launch_time < cutoff)
              ).all()

    if len(stucks):
        for stuck in stucks:
            stuck.state = 'failed'
        session.commit()
        logger.info(f"Failed {len(stucks)} flows that have been "
                    f"in a '{state}' state for {amount_of_patience} minutes from punchpipe database")


    # we clean the prefect database even if our database returned no stucks because they might have somehow gotten
    # out of sync. we want to clean that up too
    if update_prefect:
        cancel_running_prefect_flows_before_cutoff(cutoff)
