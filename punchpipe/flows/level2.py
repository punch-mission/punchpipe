from datetime import datetime
import json

from sqlalchemy import and_
from prefect import flow, task
from punchbowl.level2.flow import level2_core_flow

from punchpipe import __version__
from punchpipe.controlsegment.db import Flow, File
from punchpipe.controlsegment.processor import generic_process_flow_logic
from punchpipe.controlsegment.scheduler import generic_scheduler_flow_logic

@task
def level2_query_ready_files(session):
    return [f.file_id for f in session.query(File).where(and_(File.state == "created", File.level == 1)).all()]


@task
def level2_construct_flow_info(level1_file: File, level2_file: File):
    flow_type = "level2_process_flow"
    state = "planned"
    creation_time = datetime.now()
    priority = 1
    call_data = json.dumps({"input_filename": level1_file.filename(), "output_filename": level2_file.filename()})
    return Flow(flow_type=flow_type,
                state=state,
                flow_level=2,
                creation_time=creation_time,
                priority=priority,
                call_data=call_data)


@task
def level2_construct_file_info(level1_file: File):
    return File(level=2,
                file_type=level1_file.file_type,
                observatory=level1_file.observatory,
                file_version="0",  # TODO: decide how to implement this
                software_version= __version__,
                date_obs=level1_file.date_obs,
                polarization=level1_file.polarization,
                state="planned")


@flow
def level2_scheduler_flow():
    generic_scheduler_flow_logic(level2_query_ready_files, level2_construct_file_info, level2_construct_flow_info)


@flow
def level2_process_flow(flow_id: int):
    generic_process_flow_logic(flow_id, level2_core_flow)
