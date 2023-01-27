from datetime import datetime
import json

from prefect.context import get_run_context

from punchpipe.controlsegment.db import Flow, File
from punchpipe.controlsegment.util import get_database_session


def generic_process_flow_logic(flow_id: int, core_flow_to_launch):
    session = get_database_session()

    # fetch the appropriate flow db entry
    flow_db_entry = session.query(Flow).where(Flow.flow_id == flow_id).one()

    # update the processing flow name with the flow run name from Prefect
    flow_run_context = get_run_context()
    flow_db_entry.flow_run_name = flow_run_context.flow_run.name
    flow_db_entry.flow_run_id = flow_run_context.flow_run.id
    flow_db_entry.state = "running"
    flow_db_entry.start_time = datetime.now()
    session.commit()

    # update the file database entry as being created
    file_db_entry = session.query(File).where(File.processing_flow == flow_db_entry.flow_id).one()
    file_db_entry.state = "creating"
    session.commit()

    # load the call data and launch the core flow
    flow_call_data = json.loads(flow_db_entry.call_data)
    try:
        core_flow_to_launch(**flow_call_data)
    except Exception as e:
        flow_db_entry.state = "failed"
        file_db_entry.state = "failed"
        flow_db_entry.end_time = datetime.now()
        session.commit()
        raise e
    else:
        flow_db_entry.state = "completed"
        file_db_entry.state = "created"
        flow_db_entry.end_time = datetime.now()
        session.commit()
