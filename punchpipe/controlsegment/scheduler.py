from punchpipe.controlsegment.db import File, FileRelationship
from punchpipe.controlsegment.util import get_database_session, update_file_state, load_pipeline_configuration

def generic_scheduler_flow_logic(query_ready_files_func,
                                 construct_child_file_info,
                                 construct_child_flow_info,
                                 pipeline_config_path):
    # load pipeline configuration
    pipeline_config = load_pipeline_configuration(pipeline_config_path)

    # get database connection
    session = get_database_session()

    # find all files that are ready to run
    ready_file_ids = query_ready_files_func(session)
    for file_id in ready_file_ids:
        # mark the file as progressed so that there aren't duplicate processing flows
        update_file_state(session, file_id, "progressed")

        # get the level0 file's information
        parent_file = session.query(File).where(File.file_id == file_id).one()

        # prepare the new level 1 flow and file
        child_file = construct_child_file_info(parent_file)
        database_flow_info = construct_child_flow_info(parent_file, child_file, pipeline_config)
        session.add(child_file)
        session.add(database_flow_info)
        session.commit()

        # set the processing flow now that we know the flow_id after committing thea flow info
        child_file.processing_flow = database_flow_info.flow_id
        session.commit()

        # create a file relationship between the level 0 and level 1
        session.add(FileRelationship(parent=parent_file.file_id, child=child_file.file_id))
        session.commit()