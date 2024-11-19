import json
import typing as t
import os
from datetime import datetime

from prefect import flow, get_run_logger, task
from punchbowl.level3.f_corona_model import construct_f_corona_background
from sqlalchemy import and_

from punchpipe import __version__
from punchpipe.control.db import File, Flow
from punchpipe.control.processor import generic_process_flow_logic
from punchpipe.control.scheduler import generic_scheduler_flow_logic
from punchpipe.control.util import get_database_session


@task
def f_corona_background_query_ready_files(session, pipeline_config: dict):
    logger = get_run_logger()
    all_ready_files = (session.query(File)
                       .filter(File.state == "created")
                       .filter(File.level == "2")
                       .filter(File.file_type == "PT")
                       .filter(File.observatory == "M").all())
    logger.info(f"{len(all_ready_files)} Level 2 PTM files will be used for F corona background modeling.")
    if len(all_ready_files) > 30:  #  need at least 30 images
        return [[f.file_id for f in all_ready_files]]
    else:
        return []

@task
def construct_f_corona_background_flow_info(level3_files: list[File],
                                            level3_f_model_file: File,
                                            pipeline_config: dict,
                                            session=None):
    flow_type = "construct_f_corona_background_process_flow"
    state = "planned"
    creation_time = datetime.now()
    priority = pipeline_config["levels"][flow_type]["priority"]["initial"]
    call_data = json.dumps(
        {
            "data_list": [
                os.path.join(level3_file.directory(pipeline_config["root"]), level3_file.filename())
                for level3_file in level3_files
            ],
        }
    )
    return Flow(
        flow_type=flow_type,
        state=state,
        flow_level="3",
        creation_time=creation_time,
        priority=priority,
        call_data=call_data,
    )


@task
def construct_f_corona_background_file_info(level2_files: t.List[File], pipeline_config: dict) -> t.List[File]:
    return [File(
                level="3",
                file_type="PF",
                observatory="M",
                file_version=pipeline_config["file_version"],
                software_version=__version__,
                date_obs=level2_files[0].date_obs,  # todo use the average or something
                state="planned",
            )]

@flow
def construct_f_corona_background_scheduler_flow(pipeline_config_path=None, session=None):
    generic_scheduler_flow_logic(
        f_corona_background_query_ready_files,
        construct_f_corona_background_file_info,
        construct_f_corona_background_flow_info,
        pipeline_config_path,
        session=session,
    )

@flow
def construct_f_corona_background_process_flow(flow_id: int, pipeline_config_path=None, session=None):
    generic_process_flow_logic(flow_id, construct_f_corona_background, pipeline_config_path, session=session)