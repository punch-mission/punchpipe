import os
import json
import typing as t
from datetime import datetime

from prefect import flow, get_run_logger, task
from punchbowl.level2.flow import level2_core_flow

from punchpipe import __version__
from punchpipe.control.db import File, Flow
from punchpipe.control.processor import generic_process_flow_logic
from punchpipe.control.scheduler import generic_scheduler_flow_logic

SCIENCE_LEVEL1_TYPE_CODES = ["PM", "PZ", "PP"]# , "CR"] # TODO handle CR in a separate flow maybe


@task
def level2_query_ready_files(session, pipeline_config: dict, reference_time=None):
    logger = get_run_logger()
    all_ready_files = (session.query(File).filter(File.state == "created")
                       .filter(File.level == "1")
                       .filter(File.file_type.in_(SCIENCE_LEVEL1_TYPE_CODES)).all())
    logger.info(f"{len(all_ready_files)} ready files")
    unique_times = set(f.date_obs for f in all_ready_files)
    logger.info(f"{len(unique_times)} unique times: {unique_times}")
    grouped_ready_files = [[f.file_id for f in all_ready_files if f.date_obs == time] for time in unique_times]
    logger.info(f"{len(grouped_ready_files)} grouped ready files")
    out = [g for g in grouped_ready_files if len(g) == 12]
    logger.info(f"{len(out)} groups heading out")
    return out


@task
def level2_construct_flow_info(level1_files: list[File], level2_file: File, pipeline_config: dict, session=None, reference_time=None):
    flow_type = "level2_process_flow"
    state = "planned"
    creation_time = datetime.now()
    priority = pipeline_config["levels"][flow_type]["priority"]["initial"]
    call_data = json.dumps(
        {
            "data_list": [
                os.path.join(level1_file.directory(pipeline_config["root"]), level1_file.filename())
                for level1_file in level1_files
            ],
            "voter_filenames": [[] for _ in level1_files],
        }
    )
    return Flow(
        flow_type=flow_type,
        state=state,
        flow_level="2",
        creation_time=creation_time,
        priority=priority,
        call_data=call_data,
    )


@task
def level2_construct_file_info(level1_files: t.List[File], pipeline_config: dict, reference_time=None) -> t.List[File]:
    return [File(
                level=2,
                file_type="PT",
                observatory="M",
                file_version=pipeline_config["file_version"],
                software_version=__version__,
                date_obs=level1_files[0].date_obs,
                state="planned",
            )]


@flow
def level2_scheduler_flow(pipeline_config_path=None, session=None, reference_time=None):
    generic_scheduler_flow_logic(
        level2_query_ready_files,
        level2_construct_file_info,
        level2_construct_flow_info,
        pipeline_config_path,
        reference_time=reference_time,
        session=session,
    )


@flow
def level2_process_flow(flow_id: int, pipeline_config_path=None, session=None):
    generic_process_flow_logic(flow_id, level2_core_flow, pipeline_config_path, session=session)
