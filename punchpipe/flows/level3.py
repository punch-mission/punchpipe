import os
import json
import typing as t
from datetime import datetime, timedelta

from prefect import flow, get_run_logger, task
from punchbowl.level3.flow import level3_core_flow
from sqlalchemy import and_

from punchpipe import __version__
from punchpipe.controlsegment.db import File, Flow
from punchpipe.controlsegment.processor import generic_process_flow_logic
from punchpipe.controlsegment.scheduler import generic_scheduler_flow_logic
from punchpipe.controlsegment.util import get_database_session


def get_valid_starfields(session, f: File, timedelta_window: timedelta):
    valid_star_start, valid_star_end = f.date_obs - timedelta_window, f.date_obs + timedelta_window
    return (session.query(File).filter(File.state == "created").filter(File.level == "2")
                        .filter(File.file_type == 'PS').filter(File.observatory == 'M')
                        .filter(and_(f.date_obs >= valid_star_start,
                                     f.date_obs <= valid_star_end)).all())


def get_valid_fcorona_models(session, f: File, before_timedelta: timedelta, after_timedelta: timedelta):
    valid_fcorona_start, valid_fcorona_end = f.date_obs - before_timedelta, f.date_obs + after_timedelta
    return (session.query(File).filter(File.state == "created").filter(File.level == 2)
                      .filter(File.file_type == 'PF').filter(File.observatory == 'M')
                      .filter(and_(f.date_obs >= valid_fcorona_start,
                                   f.date_obs <= valid_fcorona_end)).all())


def get_closest_file(f_target: File, f_others: list[File]) -> File:
    return min(f_others, key=lambda o: abs((f_target.date_obs - o.date_obs).total_seconds()))

def get_closest_before_file(f_target: File, f_others: list[File]) -> File:
    return get_closest_file(f_target, [o for o in f_others if f_target.date_obs >= o.date_obs])

def get_closest_after_file(f_target: File, f_others: list[File]) -> File:
    return get_closest_file(f_target, [o for o in f_others if f_target.date_obs <= o.date_obs])

@task
def level3_PTM_query_ready_files(session, pipeline_config: dict):
    logger = get_run_logger()
    all_ready_files = session.query(File).where(and_(and_(File.state == "created",
                                                          File.level == "2"),
                                                     File.file_type == "PT")).all()
    logger.info(f"{len(all_ready_files)} Level 3 PTM files need to be processed.")

    actually_ready_files = []
    for f in all_ready_files:
        # TODO put magic numbers in config
        valid_starfields = get_valid_starfields(session, f, timedelta_window=timedelta(days=14))

        # TODO put magic numbers in config
        valid_before_fcorona_models = get_valid_fcorona_models(session, f,
                                                               before_timedelta=timedelta(days=3),
                                                               after_timedelta=timedelta(days=0))
        valid_after_fcorona_models = get_valid_fcorona_models(session, f,
                                                               before_timedelta=timedelta(days=0),
                                                               after_timedelta=timedelta(days=3))

        if (len(valid_before_fcorona_models) >= 1
                and len(valid_after_fcorona_models) >= 1
                and len(valid_starfields) >= 1):
            actually_ready_files.append(f)
    logger.info(f"{len(actually_ready_files)} Level 3 PTM files have necessary calibration data.")

    return [[f.file_id] for f in actually_ready_files]


@task
def level3_PTM_construct_flow_info(level2_files: list[File], level3_file: File, pipeline_config: dict, session=None):
    session = get_database_session()  # TODO: replace so this works in the tests by passing in a test

    flow_type = "level3_PTM_process_flow"
    state = "planned"
    creation_time = datetime.now()
    priority = pipeline_config["levels"][flow_type]["priority"]["initial"]
    call_data = json.dumps(
        {
            "data_list": [
                os.path.join(level2_file.directory(pipeline_config["root"]), level2_file.filename())
                for level2_file in level2_files
            ],
            # TODO put magic numbers in config
            "before_f_corona_model_path": get_closest_before_file(level2_files[0],
                                                    get_valid_fcorona_models(session,
                                                                             level2_files[0],
                                                                             before_timedelta=timedelta(days=3),
                                                                             after_timedelta=timedelta(days=3))),
            "after_f_corona_model_path": get_closest_after_file(level2_files[0],
                                                    get_valid_fcorona_models(session,
                                                                             level2_files[0],
                                                                             before_timedelta=timedelta(days=3),
                                                                             after_timedelta=timedelta(days=3))),
            # TODO put magic numbers in config
            "starfield_background_path": get_closest_file(level2_files[0],
                                                    get_valid_starfields(session,
                                                                             level2_files[0],
                                                                             timedelta_window=timedelta(days=14))),
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
def level3_PTM_construct_file_info(level2_files: t.List[File], pipeline_config: dict) -> t.List[File]:
    return [File(
                level="3",
                file_type="PT",
                observatory="M",
                file_version=pipeline_config["file_version"],
                software_version=__version__,
                date_obs=level2_files[0].date_obs,
                state="planned",
            )]


@flow
def level3_PTM_scheduler_flow(pipeline_config_path="config.yaml", session=None):
    generic_scheduler_flow_logic(
        level3_PTM_query_ready_files,
        level3_PTM_construct_file_info,
        level3_PTM_construct_flow_info,
        pipeline_config_path,
        session=session,
    )


@flow
def level3_PTM_process_flow(flow_id: int, pipeline_config_path="config.yaml", session=None):
    generic_process_flow_logic(flow_id, level3_core_flow, pipeline_config_path, session=session)
