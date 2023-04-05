from datetime import datetime, timedelta
import json
import os

from sqlalchemy import and_
from prefect import flow, task
from punchbowl.level2.flow import level2_core_flow

from punchpipe import __version__
from punchpipe.controlsegment.db import Flow, File
from punchpipe.controlsegment.processor import generic_process_flow_logic
from punchpipe.controlsegment.scheduler import generic_scheduler_flow_logic


@task
def level2_query_ready_files(session, pipeline_config: dict):
    latency = pipeline_config['scheduler']['level2_process_flow']['latency']
    window_duration = pipeline_config['scheduler']['level2_process_flow']['window_duration']
    start_time = datetime.now() - timedelta(minutes=latency+window_duration)
    end_time = datetime.now() - timedelta(minutes=latency)
    return [f.file_id for f in session.query(File).where(and_(File.state == "created",
                                                              File.level == 1,
                                                              File.date_obs > start_time,
                                                              File.date_obs < end_time)).all()]


@task
def level2_construct_flow_info(level1_file: File, level2_file: File, pipeline_config: dict):
    flow_type = "level2_process_flow"
    state = "planned"
    creation_time = datetime.now()
    priority = pipeline_config['priority']['level2_process_flow']['initial']
    call_data = json.dumps({"input_filename": os.path.join(level1_file.directory(pipeline_config['root']),
                                                           level1_file.filename()),
                            "output_filename": os.path.join(level2_file.directory(pipeline_config['root']),
                                                            level2_file.filename())})
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
                software_version=__version__,
                date_obs=level1_file.date_obs,
                polarization=level1_file.polarization,
                state="planned")


@flow
def level2_scheduler_flow(pipeline_config_path="config.yaml", session=None):
    generic_scheduler_flow_logic(level2_query_ready_files,
                                 level2_construct_file_info,
                                 level2_construct_flow_info,
                                 pipeline_config_path,
                                 session=session)


@flow
def level2_process_flow(flow_id: int, pipeline_config_path="config.yaml", session=None):
    generic_process_flow_logic(flow_id, level2_core_flow, pipeline_config_path, session=session)
