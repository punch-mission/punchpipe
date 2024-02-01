import json
import os
import typing as t
from datetime import datetime, timedelta

from prefect import flow, task
from punchbowl.level2.flow import level2_core_flow
from sqlalchemy import and_

from punchpipe import __version__
from punchpipe.controlsegment.db import File, Flow
from punchpipe.controlsegment.processor import generic_process_flow_logic
from punchpipe.controlsegment.scheduler import generic_scheduler_flow_logic


@task
def level2_query_ready_files(session, pipeline_config: dict):
    latency = pipeline_config['levels']['level2_process_flow']['schedule']['latency']
    window_duration = pipeline_config['levels']['level2_process_flow']['schedule']['window_duration_seconds']
    start_time = datetime.now() - timedelta(minutes=latency+window_duration)
    end_time = datetime.now() - timedelta(minutes=latency)
    return [f.file_id for f in session.query(File).where(and_(File.state == "created",
                                                              File.level == 1,
                                                              File.date_obs > start_time,
                                                              File.date_obs < end_time)).all()]


@task
def level2_construct_flow_info(level1_files: File, level2_file: File, pipeline_config: dict):
    flow_type = "level2_process_flow"
    state = "planned"
    creation_time = datetime.now()
    priority = pipeline_config['levels'][flow_type]['priority']['initial']
    call_data = json.dumps({"data_list": [os.path.join(level1_file.directory(pipeline_config['root']),
                                                           level1_file.filename()) for level1_file in level1_files]})
    return Flow(flow_type=flow_type,
                state=state,
                flow_level=2,
                creation_time=creation_time,
                priority=priority,
                call_data=call_data)


@task
def level2_construct_file_info(level1_files: t.List[File],
                               pipeline_config: dict) -> t.List[File]:
    # TODO: make realistic to level 2 products
    out_files = []
    for level1_file in level1_files:
        out_files.append(File(level=2,
                              file_type=level1_file.file_type,
                              observatory=level1_file.observatory,
                              file_version=pipeline_config['file_version'],
                              software_version=__version__,
                              date_obs=level1_file.date_obs,
                              polarization=level1_file.polarization,
                              state="planned"))
    return out_files


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
