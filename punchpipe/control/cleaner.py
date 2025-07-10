import os
from pathlib import Path

from prefect import flow, get_run_logger
from sqlalchemy.orm import aliased

from punchpipe.control.db import File, FileRelationship, Flow
from punchpipe.control.util import get_database_session, load_pipeline_configuration


@flow
def cleaner(pipeline_config_path: str, session=None):
    logger = get_run_logger()

    pipeline_config = load_pipeline_configuration(pipeline_config_path)
    if session is None:
        session = get_database_session()

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
    for _, parent, child, _ in results:
        # Handle the case that both L2 and LQ have been set to 'revivable'. If the LQ shows up first in this loop and
        # we set the L1's state to 'created', we don't want to later set it to 'quickpunched' when the L2 shows up.
        if child.level == "2" and parent.state == "progressed":
            parent.state = "quickpunched"
        else:
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
