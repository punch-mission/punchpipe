import os
import json
import typing as t
from datetime import datetime

from prefect import flow, get_run_logger, task
from punchbowl.level3.stellar import generate_starfield_background

from punchpipe import __version__
from punchpipe.control.db import File, Flow
from punchpipe.control.processor import generic_process_flow_logic
from punchpipe.control.scheduler import generic_scheduler_flow_logic


@task
def starfield_background_query_ready_files(session, pipeline_config: dict, reference_time: datetime):
    logger = get_run_logger()
    all_ready_files = (session.query(File)
                       .filter(File.state == "created")
                       .filter(File.level == "3")
                       .filter(File.file_type == "PI")
                       .filter(File.observatory == "M").all())
    logger.info(f"{len(all_ready_files)} Level 3 PIM files will be used for F corona background modeling.")
    if len(all_ready_files) >= 30:
        return [[f.file_id for f in all_ready_files]]
    else:
        return []


@task
def construct_starfield_background_flow_info(level3_fcorona_subtracted_files: list[File],
                                             level3_starfield_model_file: File,
                                             pipeline_config: dict,
                                             reference_time: datetime,
                                             session=None ):
    flow_type = "construct_starfield_background_process_flow"
    state = "planned"
    creation_time = datetime.now()
    priority = pipeline_config["levels"][flow_type]["priority"]["initial"]
    call_data = json.dumps(
        {
            "filenames": list(set([
                os.path.join(level3_file.directory(pipeline_config["root"]), level3_file.filename())
                for level3_file in level3_fcorona_subtracted_files
            ])),
            "reference_time": str(reference_time)
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
def construct_starfield_background_file_info(level3_files: t.List[File], pipeline_config: dict,
                                             reference_time: datetime) -> t.List[File]:
    return [File(
                level="3",
                file_type="PS",
                observatory="M",
                file_version=pipeline_config["file_version"],
                software_version=__version__,
                date_obs= reference_time,
                state="planned",
            )
    ]


@flow
def starfield_scheduler_flow(pipeline_config_path=None, session=None, reference_time: datetime | None = None):
    reference_time = reference_time or datetime.now()

    generic_scheduler_flow_logic(
        starfield_background_query_ready_files,
        construct_starfield_background_file_info,
        construct_starfield_background_flow_info,
        pipeline_config_path,
        update_input_file_state=False,
        reference_time=reference_time,
        session=session,
    )


@flow
def starfield_process_flow(flow_id: int, pipeline_config_path=None, session=None):
    generic_process_flow_logic(flow_id,
                               generate_starfield_background,
                               pipeline_config_path,
                               session=session)
