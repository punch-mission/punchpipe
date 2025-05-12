import inspect
from datetime import datetime

from prefect import get_run_logger

from punchpipe.control.db import File, FileRelationship
from punchpipe.control.util import get_database_session, load_pipeline_configuration, update_file_state


def generic_scheduler_flow_logic(
    query_ready_files_func, construct_child_file_info, construct_child_flow_info, pipeline_config_path,
        update_input_file_state=True, new_input_file_state="progressed",
        session=None, reference_time: datetime | None = None,
):
    logger = get_run_logger()
    pipeline_config = load_pipeline_configuration(pipeline_config_path)

    max_start = pipeline_config['scheduler']['max_start']

    if session is None:
        session = get_database_session()


    # Not every level*_query_ready_files function needs this max_n parameter---some instead have a use_n that's similar
    # at first glance, but fills a different role and needs to be tuned differently. To avoid confusion there, we don't
    # require every implementation to accept a max_n parameter---instead, we send that parameter only to those functions
    # that accept it.
    if 'max_n' in inspect.signature(query_ready_files_func).parameters:
        extra_args = {'max_n': max_start}
    else:
        extra_args = {}
    # find all files that are ready to run
    ready_file_ids = query_ready_files_func(
        session, pipeline_config, reference_time=reference_time, **extra_args)[:max_start]
    logger.info(f"Got {len(ready_file_ids)} groups of ready files")
    if ready_file_ids:
        for group in ready_file_ids:
            parent_files = []
            for file_id in group:
                # mark the file as progressed so that there aren't duplicate processing flows
                if update_input_file_state:
                    update_file_state(session, file_id, new_input_file_state)

                # get the prior level file's information
                parent_files += session.query(File).where(File.file_id == file_id).all()

            # prepare the new level flow and file
            children_files = construct_child_file_info(parent_files, pipeline_config, reference_time=reference_time)
            database_flow_info = construct_child_flow_info(parent_files, children_files,
                                                           pipeline_config, session=session,
                                                           reference_time=reference_time)
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
