import inspect
import itertools
from datetime import datetime

from prefect import get_run_logger

from punchpipe.control.db import File, FileRelationship
from punchpipe.control.util import get_database_session, load_pipeline_configuration, update_file_state


def generic_scheduler_flow_logic(
    query_ready_files_func, construct_child_file_info, construct_child_flow_info, pipeline_config_path,
        update_input_file_state=True, new_input_file_state="progressed",
        session=None, reference_time: datetime | None = None,
        args_dictionary: dict = {},
        children_are_one_to_one: bool = False,
    ):
    """
    Implement the core logic of each scheduler flow.

    Parameters
    ----------
    query_ready_files_func
        A function that returns a list of lists, where each of the inner lists is a group of files that will be the
        inputs to one flow
    query_ready_files_func
        A function that generates the child File entries for one group/flow
    construct_child_flow_info
        A function that generates the Flow entry for one group
    pipeline_config_path
        The config path
    update_input_file_state
        Whether to change the state of the input files
    new_input_file_state
        The new state to assign to each input file
    session
        A database Session
    reference_time
        Some time of observation time associated with this scheduling run. The meaning is defined by flow-specific
        functions passed into this function.
    args_dictionary: dict
         Values in this dictionary are passed directly to the 
         `query_ready_files_func`, `query_ready_files_func`, and `construct_child_flow_info` functions
    children_are_one_to_one
        By default, for each group of input files, it is assumed that all inputs together produce all the output
        files, and FileRelationships are generated accordingly. In a case where a batch of input files are to be
        processed in one flow, this assumption doesn't hold. When this flag is set to True, it is assumed each input
        file connects to only one output file (at the corresponding position in the list of child File objects).
    """

    logger = get_run_logger()
    pipeline_config = load_pipeline_configuration(pipeline_config_path)

    # Extract the calling flow's type from the name of the calling function. The calling function's name is fixed by
    # the logic in cli.py that finds the code for a flow named in the configuration file.
    calling_function = inspect.currentframe().f_back.f_code.co_qualname
    if "_scheduler_flow" in calling_function:
        flow_type = calling_function.replace('_scheduler_flow', '')
        logger.info(f"This is flow type {flow_type}")
        if not pipeline_config["flows"][flow_type].get("enabled", True):
            logger.info(f"Flow {flow_type} is not enabled---halting scheduler")
            return

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
        session, pipeline_config, reference_time=reference_time, **extra_args, **args_dictionary)[:max_start]
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
            children_files = construct_child_file_info(parent_files, pipeline_config, reference_time=reference_time, **args_dictionary)
            database_flow_info = construct_child_flow_info(parent_files, children_files,
                                                           pipeline_config, session=session,
                                                           reference_time=reference_time, **args_dictionary)
            for child_file in children_files:
                session.add(child_file)
            session.add(database_flow_info)
            session.commit()

            # set the processing flow now that we know the flow_id after committing the flow info
            for child_file in children_files:
                child_file.processing_flow = database_flow_info.flow_id
            session.commit()

            # create a file relationship between the prior and next levels
            if children_are_one_to_one:
                iterable = zip(parent_files, children_files)
            else:
                iterable = itertools.product(parent_files, children_files)
            for parent_file, child_file in iterable:
                session.add(FileRelationship(parent=parent_file.file_id, child=child_file.file_id))
            session.commit()
