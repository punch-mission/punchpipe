import os
import json
import typing as t
from datetime import UTC, datetime, timedelta

from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from punchbowl.level1.stray_light import estimate_stray_light

from punchpipe import __version__
from punchpipe.control.db import File, Flow
from punchpipe.control.processor import generic_process_flow_logic
from punchpipe.control.scheduler import generic_scheduler_flow_logic


@task(cache_policy=NO_CACHE)
def construct_stray_light_query_ready_files(session,
                                            pipeline_config: dict,
                                            reference_time: datetime,
                                            spacecraft: str,
                                            file_type: str):
    before = reference_time - timedelta(weeks=1)

    file_type_mapping = {"SR": "XR", "SM": "XM", "SZ": "XZ", "SP": "XP"}
    target_file_type = file_type_mapping[file_type]

    logger = get_run_logger()
    all_ready_files = (session.query(File)
                       .filter(File.state.in_(["created", "progressed", "quickpunched"]))
                       .filter(File.date_obs >= before)
                       .filter(File.date_obs <= reference_time)
                       .filter(File.level == "1")
                       .filter(File.file_type == target_file_type)
                       .filter(File.observatory == spacecraft)
                       .order_by(File.date_obs.desc())
                       .limit(1000).all())
    logger.info(f"{len(all_ready_files)} Level 1 {target_file_type}{spacecraft} files will be used for stray light estimation.")
    if len(all_ready_files) > 30:  #  need at least 30 images
        return [[f.file_id for f in all_ready_files]]
    else:
        return []

@task(cache_policy=NO_CACHE)
def construct_stray_light_flow_info(level1_files: list[File],
                                    level1_stray_light_file: File,
                                    pipeline_config: dict,
                                    reference_time: datetime,
                                    file_type: str,
                                    spacecraft: str,
                                    session=None):
    flow_type = "construct_stray_light"
    state = "planned"
    creation_time = datetime.now()
    priority = pipeline_config["flows"][flow_type]["priority"]["initial"]
    call_data = json.dumps(
        {
            "data_root": pipeline_config["root"],
            "filepaths": [
                os.path.join(level1_file.directory(''), level1_file.filename())
                for level1_file in level1_files
            ],
        }
    )
    return Flow(
        flow_type=flow_type,
        state=state,
        flow_level="1",
        creation_time=creation_time,
        priority=priority,
        call_data=call_data,
    )


@task(cache_policy=NO_CACHE)
def construct_stray_light_file_info(level1_files: t.List[File],
                                    pipeline_config: dict,
                                    reference_time: datetime,
                                    file_type: str,
                                    spacecraft: str) -> t.List[File]:
    date_obses = [f.date_obs for f in level1_files]
    first_dateobs = sorted(date_obses)[0]
    return [File(
                level="1",
                file_type=file_type,
                observatory=spacecraft,
                file_version=pipeline_config["file_version"],
                software_version=__version__,
                date_obs=first_dateobs,
                state="planned",
            ),]

@flow
def construct_stray_light_scheduler_flow(pipeline_config_path=None, session=None, reference_time: datetime | None = None):
    reference_time = reference_time or datetime.now(UTC)

    for file_type in ["SR", "SM", "SZ", "SP"]:
        for spacecraft in ["1", "2", "3", "4"]:

            args_dictionary = {"file_type": file_type, "spacecraft": spacecraft}

            generic_scheduler_flow_logic(
                construct_stray_light_query_ready_files,
                construct_stray_light_file_info,
                construct_stray_light_flow_info,
                pipeline_config_path,
                update_input_file_state=False,
                reference_time=reference_time,
                session=session,
                args_dictionary=args_dictionary
            )


def construct_stray_light_call_data_processor(call_data: dict, pipeline_config, session) -> dict:
    # Prepend the data root to each input file
    call_data['filepaths'] = [os.path.join(call_data['data_root'], f) for f in call_data['filepaths']]
    del call_data['data_root']
    return call_data

@flow
def construct_stray_light_process_flow(flow_id: int, pipeline_config_path=None, session=None):
    generic_process_flow_logic(flow_id, estimate_stray_light, pipeline_config_path, session=session,
                               call_data_processor=construct_stray_light_call_data_processor)
