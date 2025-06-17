import json
from datetime import datetime

from prefect import get_run_logger
from prefect.context import get_run_context

from punchpipe.control.db import File, Flow
from punchpipe.control.util import (
    get_database_session,
    load_pipeline_configuration,
    match_data_with_file_db_entry,
    write_file,
)


def generic_process_flow_logic(flow_id: int, core_flow_to_launch, pipeline_config_path: str, session=None,
                               call_data_processor=None):
    if session is None:
        session = get_database_session()
    try:
        logger = get_run_logger()

        # load pipeline configuration
        pipeline_config = load_pipeline_configuration(pipeline_config_path)

        # fetch the appropriate flow db entry
        flow_db_entry = session.query(Flow).where(Flow.flow_id == flow_id).one()
        if flow_db_entry.state != "launched":
            logger.warning(f"Flow id {flow_db_entry.flow_id} has state '{flow_db_entry.state}'; not running")
            return
        logger.info(f"Running on flow db entry with id={flow_db_entry.flow_id}.")

        # update the processing flow name with the flow run name from Prefect
        flow_run_context = get_run_context()
        flow_db_entry.flow_run_name = flow_run_context.flow_run.name
        flow_db_entry.flow_run_id = flow_run_context.flow_run.id
        flow_db_entry.state = "running"
        flow_db_entry.start_time = datetime.now()

        file_db_entry_list = session.query(File).where(File.processing_flow == flow_db_entry.flow_id).all()

        # update the file database entries as being created
        if file_db_entry_list:
            for file_db_entry in file_db_entry_list:
                if file_db_entry.state != "planned":
                    raise RuntimeError(f"File id {file_db_entry.file_id} has already been created.")
                file_db_entry.state = "creating"
        else:
            raise RuntimeError("There should be at least one file associated with this flow. Found 0.")
        session.commit()

        # load the call data and launch the core flow
        flow_call_data = json.loads(flow_db_entry.call_data)
        if call_data_processor is not None:
            flow_call_data = call_data_processor(flow_call_data, pipeline_config, session)
        output_file_ids = set()
        expected_file_ids = {entry.file_id for entry in file_db_entry_list}
        logger.info(f"Expecting to output files with ids={expected_file_ids}.")

        results = core_flow_to_launch(**flow_call_data)
        for result in results:
            file_db_entry = match_data_with_file_db_entry(result, file_db_entry_list)
            logger.info(f"Preparing to write {file_db_entry.file_id}.")
            output_file_ids.add(file_db_entry.file_id)
            filename = write_file(result, file_db_entry, pipeline_config)
            logger.info(f"Wrote to {filename}")

        for file_id in expected_file_ids.difference(output_file_ids):
            entry = session.query(File).where(File.file_id == file_id).one()
            entry.state = "unreported"

        flow_db_entry.state = "completed"
        flow_db_entry.end_time = datetime.now()
        # Note: the file_db_entry gets updated above in the writing step because it could be created or blank
        session.commit()
    except:
        session.query(Flow).filter(Flow.flow_id == flow_id).update(
            {"state": "failed",
             "end_time": datetime.now()})
        session.query(File).filter(File.processing_flow == flow_id).update(
            {"state": "failed"})
        session.commit()
        raise
