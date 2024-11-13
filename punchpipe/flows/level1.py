import os
import json
import typing as t
from datetime import datetime

from prefect import flow, task
from punchbowl.level1.flow import level1_core_flow
from sqlalchemy import and_

from punchpipe import __version__
from punchpipe.controlsegment.db import File, Flow
from punchpipe.controlsegment.processor import generic_process_flow_logic
from punchpipe.controlsegment.scheduler import generic_scheduler_flow_logic

SCIENCE_LEVEL0_TYPE_CODES = ["PM", "PZ", "PP", "CR"]

@task
def level1_query_ready_files(session, pipeline_config: dict):
    return [[f.file_id] for f in session.query(File).filter(File.file_type.in_(SCIENCE_LEVEL0_TYPE_CODES))
    .where(and_(File.state == "created",  File.level == "0")).all()]


@task
def get_psf_model_path(level0_file, pipeline_config: dict, session=None):
    corresponding_psf_model_type = {"PM": "RM",
                                    "PZ": "RZ",
                                    "PP": "RP",
                                    "CR": "RC"}
    psf_model_type = corresponding_psf_model_type[level0_file.file_type]
    best_model = (session.query(File)
                  .filter(File.file_type == psf_model_type)
                  .filter(File.observatory == level0_file.observatory)
                  .where(File.date_obs <= level0_file.date_obs)
                  .order_by(File.date_obs.desc()).first())
    return os.path.join(best_model.directory(pipeline_config['root']), best_model.filename())

@task
def get_quartic_model_path(level0_file, pipeline_config: dict, session=None):
    best_model = (session.query(File)
                  .filter(File.file_type == 'FQ')
                  .filter(File.observatory == level0_file.observatory)
                  .where(File.date_obs <= level0_file.date_obs)
                  .order_by(File.date_obs.desc()).first())
    return os.path.join(best_model.directory(pipeline_config['root']), best_model.filename())

@task
def level1_construct_flow_info(level0_files: list[File], level1_files: File, pipeline_config: dict, session=None):
    flow_type = "level1_process_flow"
    state = "planned"
    creation_time = datetime.now()
    priority = pipeline_config["levels"][flow_type]["priority"]["initial"]
    call_data = json.dumps(
        {
            "input_data": [
                os.path.join(level0_file.directory(pipeline_config["root"]), level0_file.filename())
                for level0_file in level0_files
            ],
            "psf_model_path": get_psf_model_path(level0_files[0], pipeline_config, session=session),
            "quartic_coefficient_path": get_quartic_model_path(level0_files[0], pipeline_config, session=session),
        }
    )
    return Flow(
        flow_type=flow_type,
        flow_level="1",
        state=state,
        creation_time=creation_time,
        priority=priority,
        call_data=call_data,
    )


@task
def level1_construct_file_info(level0_files: t.List[File], pipeline_config: dict) -> t.List[File]:
    return [
        File(
            level="1",
            file_type=level0_files[0].file_type,
            observatory=level0_files[0].observatory,
            file_version=pipeline_config["file_version"],
            software_version=__version__,
            date_obs=level0_files[0].date_obs,
            polarization=level0_files[0].polarization,
            state="planned",
        )
    ]


@flow
def level1_scheduler_flow(pipeline_config_path="config.yaml", session=None):
    generic_scheduler_flow_logic(
        level1_query_ready_files,
        level1_construct_file_info,
        level1_construct_flow_info,
        pipeline_config_path,
        session=session,
    )


@flow
def level1_process_flow(flow_id: int, pipeline_config_path="config.yaml", session=None):
    generic_process_flow_logic(flow_id, level1_core_flow, pipeline_config_path, session=session)
