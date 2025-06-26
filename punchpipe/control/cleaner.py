import os

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
    for child in children:
        output_path = os.path.join(
            child.directory(pipeline_config["root"]), child.filename()
        )
        if os.path.exists(output_path):
            os.remove(output_path)
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
