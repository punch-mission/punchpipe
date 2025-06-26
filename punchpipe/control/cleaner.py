import os
from pathlib import Path

from prefect import flow, get_run_logger

from punchpipe.control.db import File, FileRelationship, Flow
from punchpipe.control.util import get_database_session, load_pipeline_configuration


@flow
def cleaner(pipeline_config_path: str):
    logger = get_run_logger()

    pipeline_config = load_pipeline_configuration(pipeline_config_path)
    session = get_database_session()

    # Note: I thought about adding a maximum here, but this flow takes only 5 seconds to revive 10,000 L1 flows, so I
    # think we're good.
    flows = (session.query(Flow).where(Flow.state == 'revivable')
                                .all())
    flow_ids = [flow.flow_id for flow in flows]
    children = session.query(File).where(File.processing_flow.in_(flow_ids)).all()
    children_ids = [child.file_id for child in children]
    parents = (session.query(File).join(FileRelationship, File.file_id == FileRelationship.parent)
                      .where(FileRelationship.child.in_(children_ids)).all())
    relationships = session.query(FileRelationship).where(FileRelationship.child.in_(children_ids)).all()

    logger.info(f"Resetting {len(parents)} parent files")
    for parent in parents:
        parent.state = "created"

    logger.info(f"Deleting {len(children)} child files")
    root_path = Path(pipeline_config["root"])
    for child in children:
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

    logger.info(f"Clearing {len(relationships)} file relationships")
    for relationship in relationships:
        session.delete(relationship)

    logger.info(f"Deleting {len(flows)} flows")
    for f in flows:
        session.delete(f)

    session.commit()
    if len(flows):
        logger.info(f"Revived {len(flows)} flows")
