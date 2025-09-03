import os
import json
import typing as t
from datetime import datetime

from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from punchbowl.level1.flow import levelh_core_flow
from sqlalchemy.orm import aliased

from punchpipe import __version__
from punchpipe.control.db import File, FileRelationship, Flow
from punchpipe.control.processor import generic_process_flow_logic
from punchpipe.control.scheduler import generic_scheduler_flow_logic
from punchpipe.flows.level1 import (
    SCIENCE_LEVEL1_LATE_OUTPUT_TYPE_CODES,
    get_ccd_parameters,
    get_distortion_path,
    get_psf_model_path,
)
from punchpipe.flows.util import file_name_to_full_path


@task(cache_policy=NO_CACHE)
def levelh_query_ready_files(session, pipeline_config: dict, reference_time=None, max_n=9e99):
    logger = get_run_logger()
    parent = aliased(File)
    child = aliased(File)
    child_exists_subquery = (session.query(parent)
                             .join(FileRelationship, FileRelationship.parent == parent.file_id)
                             .join(child, FileRelationship.child == child.file_id)
                             .filter(parent.file_id == File.file_id)
                             .filter(child.level == "H")
                             .exists())
    ready = (session.query(File)
             .filter(File.file_type.in_(SCIENCE_LEVEL1_LATE_OUTPUT_TYPE_CODES))
             .filter(File.level == "0")
             .filter(File.state.in_(["created", "progressed"]))
             .filter(~child_exists_subquery)
             .order_by(File.date_obs.desc()).all())
    actually_ready = []
    for f in ready:
        if get_psf_model_path(f, pipeline_config, session=session) is None:
            logger.info(f"Missing PSF for {f.filename()}")
            continue
        actually_ready.append([f])
        if len(actually_ready) >= max_n:
            break
    return actually_ready

@task(cache_policy=NO_CACHE)
def levelh_construct_flow_info(level0_files: list[File], level1_files: File,
                               pipeline_config: dict, session=None, reference_time=None):
    flow_type = "levelh"
    state = "planned"
    creation_time = datetime.now()
    priority = pipeline_config["flows"][flow_type]["priority"]["initial"]

    best_psf_model = get_psf_model_path(level0_files[0], pipeline_config, session=session)
    ccd_parameters = get_ccd_parameters(level0_files[0], pipeline_config, session=session)
    best_distortion = get_distortion_path(level0_files[0], pipeline_config, session=session)

    call_data = json.dumps(
        {
            "input_data": [level0_file.filename() for level0_file in level0_files],
            "psf_model_path": best_psf_model.filename(),
            "gain_bottom": ccd_parameters['gain_bottom'],
            "gain_top": ccd_parameters['gain_top'],
            "distortion_path": os.path.join(best_distortion.directory(pipeline_config['root']),
                                            best_distortion.filename()),
        }
    )
    return Flow(
        flow_type=flow_type,
        flow_level="H",
        state=state,
        creation_time=creation_time,
        priority=priority,
        call_data=call_data,
    )


@task
def levelh_construct_file_info(level0_files: t.List[File], pipeline_config: dict, reference_time=None) -> t.List[File]:
    return [
        File(
            level="H",
            file_type=level0_files[0].file_type,
            observatory=level0_files[0].observatory,
            file_version=pipeline_config["file_version"],
            software_version=__version__,
            date_obs=level0_files[0].date_obs,
            polarization=level0_files[0].polarization,
            outlier=level0_files[0].outlier,
            state="planned",
        )
    ]


@flow
def levelh_scheduler_flow(pipeline_config_path=None, session=None, reference_time=None):
    generic_scheduler_flow_logic(
        levelh_query_ready_files,
        levelh_construct_file_info,
        levelh_construct_flow_info,
        pipeline_config_path,
        reference_time=reference_time,
        session=session,
        update_input_file_state=False
    )


def levelh_call_data_processor(call_data: dict, pipeline_config, session=None) -> dict:
    for key in ['input_data', 'psf_model_path']:
        call_data[key] = file_name_to_full_path(call_data[key], pipeline_config['root'])

    # Anything more than 16 doesn't offer any real benefit, and the default of n_cpu on punch190 is actually slower than
    # 16! Here we choose less to have less spiky CPU usage to play better with other flows.
    call_data['max_workers'] = 2
    return call_data


@flow
def levelh_process_flow(flow_id: int, pipeline_config_path=None, session=None):
    generic_process_flow_logic(flow_id, levelh_core_flow, pipeline_config_path, session=session,
                               call_data_processor=levelh_call_data_processor)
