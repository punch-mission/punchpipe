import os
import json
import typing as t
from datetime import UTC, datetime

from prefect import flow, get_run_logger, task
from punchbowl.level3.flow import generate_level3_low_noise_flow
from sqlalchemy import and_

from punchpipe import __version__
from punchpipe.control.db import File, Flow
from punchpipe.control.processor import generic_process_flow_logic
from punchpipe.control.scheduler import generic_scheduler_flow_logic

# TODO - repeat below logic for the final PAM set

@task
def level3_CAM_query_ready_files(session, pipeline_config: dict, reference_time=None, max_n=9e99):
    logger = get_run_logger()
    all_ready_files = session.query(File).where(and_(and_(File.state.in_(["progressed", "created"]),
                                                          File.level == "3"),
                                                     File.file_type == "CT")).order_by(File.date_obs.asc()).all()
    # TODO - need to grab data from sets of rotation. look at movie processor for inspiration
    logger.info(f"{len(all_ready_files)} Level 3 CTM files need to be processed to low-noise.")

    actually_ready_files = []
    for f in all_ready_files:
        # TODO - remove this unless the time check happens here
        actually_ready_files.append(f)
        if len(actually_ready_files) >= max_n:
            break
    logger.info(f"{len(actually_ready_files)} Level 3 CTM files selected for low-noise processing")

    return [[f.file_id] for f in actually_ready_files]


@task
def level3_CAM_construct_flow_info(level3_files: list[File], level3_file_out: File,
                                   pipeline_config: dict, session=None, reference_time=None):

    flow_type = "level3_CAM"
    state = "planned"
    creation_time = datetime.now(UTC)
    priority = pipeline_config["flows"][flow_type]["priority"]["initial"]

    call_data = json.dumps(
        {
            "data_list": [
                os.path.join(level3_file.directory(pipeline_config["root"]), level3_file.filename())
                for level3_file in level3_files
            ]
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
def level3_CAM_construct_file_info(level3_files: t.List[File], pipeline_config: dict, reference_time=None) -> t.List[File]:
    return [File(
                level="3",
                file_type="CA",
                observatory="M",
                file_version=pipeline_config["file_version"],
                software_version=__version__,
                date_obs=level3_files[0].date_obs, # TODO - set to date avg
                state="planned",
            )]


@flow
def level3_CAM_scheduler_flow(pipeline_config_path=None, session=None, reference_time=None):
    generic_scheduler_flow_logic(
        level3_CAM_query_ready_files,
        level3_CAM_construct_file_info,
        level3_CAM_construct_flow_info,
        pipeline_config_path,
        reference_time=reference_time,
        session=session,
    )


@flow
def level3_CAM_process_flow(flow_id: int, pipeline_config_path=None, session=None):
    generic_process_flow_logic(flow_id, generate_level3_low_noise_flow, pipeline_config_path, session=session)
