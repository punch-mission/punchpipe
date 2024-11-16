from prefect.variables import Variable

from punchpipe.controlsegment.db import File, FileRelationship
from punchpipe.controlsegment.util import get_database_session, load_pipeline_configuration, update_file_state


def generic_scheduler_flow_logic(
    query_ready_files_func, construct_child_file_info, construct_child_flow_info, pipeline_config_path, session=None
):
    pipeline_config = load_pipeline_configuration(pipeline_config_path)

    max_start = pipeline_config['scheduler']['max_start']

    # get database connection
    if session is None:
        session = get_database_session()

    # find all files that are ready to run
    ready_file_ids = query_ready_files_func(session, pipeline_config)[:max_start]
    if ready_file_ids:
        for group in ready_file_ids:
            parent_files = []
            for file_id in group:
                # mark the file as progressed so that there aren't duplicate processing flows
                update_file_state(session, file_id, "progressed")

                # get the prior level file's information
                parent_files += session.query(File).where(File.file_id == file_id).all()

            # prepare the new level flow and file
            children_files = construct_child_file_info(parent_files, pipeline_config)
            database_flow_info = construct_child_flow_info(parent_files, children_files,
                                                           pipeline_config, session=session)
            for child_file in children_files:
                session.add(child_file)
            session.add(database_flow_info)
            session.commit()

            # set the processing flow now that we know the flow_id after committing the flow info
            for child_file in children_files:
                child_file.processing_flow = database_flow_info.flow_id
            session.commit()

            # create a file relationship between the prior and next levels
            for parent_file in parent_files:
                for child_file in children_files:
                    session.add(FileRelationship(parent=parent_file.file_id, child=child_file.file_id))
            session.commit()
