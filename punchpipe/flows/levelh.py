import os
import json
import typing as t
from datetime import datetime

from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from punchbowl.level1.flow import levelh_core_flow

from punchpipe import __version__
from punchpipe.control.db import File, Flow
from punchpipe.control.processor import generic_process_flow_logic
from punchpipe.control.scheduler import generic_scheduler_flow_logic
from punchpipe.flows.level1 import get_ccd_parameters, get_psf_model_path

SCIENCE_LEVEL0_TYPE_CODES = ["PM", "PZ", "PP", "CR"]

@task(cache_policy=NO_CACHE)
def levelh_query_ready_files(session, pipeline_config: dict, reference_time=None, max_n=9e99):
    ready = [f for f in session.query(File).filter(File.file_type.in_(SCIENCE_LEVEL0_TYPE_CODES))
                                           .filter(File.state == "quickpunched")
                                           .filter(File.level == "0")
                                           .order_by(File.date_obs.asc()).all()]
    actually_ready = []
    for f in ready:
        if get_psf_model_path(f, pipeline_config, session=session) is not None:
            actually_ready.append([f.file_id])
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

    call_data = json.dumps(
        {
            "input_data": [
                os.path.join(level0_file.directory(pipeline_config["root"]), level0_file.filename())
                for level0_file in level0_files
            ],
            "psf_model_path": os.path.join(best_psf_model.directory(pipeline_config['root']),
                                           best_psf_model.filename()),
            "gain_bottom": ccd_parameters['gain_bottom'],
            "gain_top": ccd_parameters['gain_top']
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
        new_input_file_state="quickpunched"
    )


@flow
def levelh_process_flow(flow_id: int, pipeline_config_path=None, session=None):
    generic_process_flow_logic(flow_id, levelh_core_flow, pipeline_config_path, session=session)
