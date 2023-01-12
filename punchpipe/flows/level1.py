from datetime import datetime
import json

from sqlalchemy import and_
from prefect import flow, task
from prefect.context import get_run_context
from punchbowl.level1.flow import level1_core_flow

from punchpipe import __version__
from punchpipe.controlsegment.db import Flow, File
from punchpipe.controlsegment.processor import generic_process_flow_logic
from punchpipe.controlsegment.scheduler import generic_scheduler_flow_logic

@task
def level1_query_ready_files(session):
    return [f.file_id for f in session.query(File).where(and_(File.state == "created", File.level == 0)).all()]


@task
def level1_construct_flow_info(level0_file: File, level1_file: File):
    flow_type = "level1_process_flow"
    state = "planned"
    creation_time = datetime.now()
    priority = 1
    call_data = json.dumps({"input_filename": level0_file.filename(), "output_filename": level1_file.filename()})
    return Flow(flow_type=flow_type,
                flow_level=1,
                state=state,
                creation_time=creation_time,
                priority=priority,
                call_data=call_data)


@task
def level1_construct_file_info(level0_file: File):
    return File(level=1,
                file_type=level0_file.file_type,
                observatory=level0_file.observatory,
                file_version="0",  # TODO: decide how to implement this
                software_version=__version__,
                date_obs=level0_file.date_obs,
                polarization=level0_file.polarization,
                state="planned")


@flow
def level1_scheduler_flow():
    generic_scheduler_flow_logic(level1_query_ready_files, level1_construct_file_info, level1_construct_flow_info)


@flow
def level1_process_flow(flow_id: int):
    generic_process_flow_logic(flow_id, level1_core_flow)
