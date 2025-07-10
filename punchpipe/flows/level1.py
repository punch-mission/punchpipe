import os
import json
import typing as t
from datetime import datetime

from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from punchbowl.level1.flow import level1_core_flow

from punchpipe import __version__
from punchpipe.control import cache_layer
from punchpipe.control.db import File, Flow
from punchpipe.control.processor import generic_process_flow_logic
from punchpipe.control.scheduler import generic_scheduler_flow_logic

SCIENCE_LEVEL0_TYPE_CODES = ["PM", "PZ", "PP", "CR"]

@task(cache_policy=NO_CACHE)
def level1_query_ready_files(session, pipeline_config: dict, reference_time=None, max_n=9e99):
    logger = get_run_logger()
    ready = (session.query(File).filter(File.file_type.in_(SCIENCE_LEVEL0_TYPE_CODES))
                                .filter(File.state == "created")
                                .filter(File.level == "0")
                                .order_by(File.date_obs.asc()).all())

    actually_ready = []
    for f in ready:
        if get_psf_model_path(f, pipeline_config, session=session) is None:
            logger.info(f"Missing PSF for {f.filename()}")
            continue
        if get_quartic_model_path(f, pipeline_config, session=session) is None:
            logger.info(f"Missing quartic model for {f.filename()}")
            continue
        if get_stray_light(f, pipeline_config, session=session) is None:
            logger.info(f"Missing stray light model for {f.filename()}")
            continue
        if get_vignetting_function_path(f, pipeline_config, session=session) is None:
            logger.info(f"Missing vignetting function for {f.filename()}")
            continue
        if get_distortion_path(f, pipeline_config, session=session) is None:
            logger.info(f"Missing distortion function for {f.filename()}")
            continue
        actually_ready.append([f.file_id])
        if len(actually_ready) >= max_n:
            break
    return actually_ready

def get_distortion_path(level0_file, pipeline_config: dict, session=None, reference_time=None):
    best_function = (session.query(File)
                     .filter(File.file_type == "DS")
                     .filter(File.observatory == level0_file.observatory)
                     .where(File.date_obs <= level0_file.date_obs)
                     .order_by(File.date_obs.desc()).first())
    return best_function

def get_vignetting_function_path(level0_file, pipeline_config: dict, session=None, reference_time=None):
    corresponding_vignetting_function_type = {"PM": "GM",
                                              "PZ": "GZ",
                                              "PP": "GP",
                                              "CR": "GR"}
    vignetting_function_type = corresponding_vignetting_function_type[level0_file.file_type]
    best_function = (session.query(File)
                     .filter(File.file_type == vignetting_function_type)
                     .filter(File.observatory == level0_file.observatory)
                     .where(File.date_obs <= level0_file.date_obs)
                     .order_by(File.date_obs.desc()).first())
    return best_function


def get_psf_model_path(level0_file, pipeline_config: dict, session=None, reference_time=None):
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
    return best_model

def get_stray_light(level0_file, pipeline_config: dict, session=None, reference_time=None):
    corresponding_type = {"PM": "SM",
                          "PZ": "SZ",
                          "PP": "SP",
                          "CR": "SR"}
    model_type = corresponding_type[level0_file.file_type]
    best_model = (session.query(File)
                  .filter(File.file_type == model_type)
                  .filter(File.observatory == level0_file.observatory)
                  .where(File.date_obs <= level0_file.date_obs)
                  .order_by(File.date_obs.desc()).first())
    return best_model


def get_quartic_model_path(level0_file, pipeline_config: dict, session=None, reference_time=None):
    best_model = (session.query(File)
                  .filter(File.file_type == 'FQ')
                  .filter(File.observatory == level0_file.observatory)
                  .where(File.date_obs <= level0_file.date_obs)
                  .order_by(File.date_obs.desc()).first())
    return best_model


def get_mask_file(level0_file, pipeline_config: dict, session=None, reference_time=None):
    best_model = (session.query(File)
                  .filter(File.file_type == 'MS')
                  .filter(File.observatory == level0_file.observatory)
                  .where(File.date_obs <= level0_file.date_obs)
                  .order_by(File.date_obs.desc()).first())
    return best_model


def get_ccd_parameters(level0_file, pipeline_config: dict, session=None):
    gain_bottom, gain_top = pipeline_config['ccd_gain'][int(level0_file.observatory)]
    return {"gain_bottom": gain_bottom, "gain_top": gain_top}


def level1_construct_flow_info(level0_files: list[File], level1_files: File,
                               pipeline_config: dict, session=None, reference_time=None):
    flow_type = "level1"
    state = "planned"
    creation_time = datetime.now()
    priority = pipeline_config["flows"][flow_type]["priority"]["initial"]

    best_vignetting_function = get_vignetting_function_path(level0_files[0], pipeline_config, session=session)
    best_psf_model = get_psf_model_path(level0_files[0], pipeline_config, session=session)
    best_quartic_model = get_quartic_model_path(level0_files[0], pipeline_config, session=session)
    best_distortion = get_distortion_path(level0_files[0], pipeline_config, session=session)
    ccd_parameters = get_ccd_parameters(level0_files[0], pipeline_config, session=session)
    best_stray_light = get_stray_light(level0_files[0], pipeline_config, session=session)
    mask_function = get_mask_file(level0_files[0], pipeline_config, session=session)

    call_data = json.dumps(
        {
            "input_data": [
                os.path.join(level0_file.directory(pipeline_config["root"]), level0_file.filename())
                for level0_file in level0_files
            ],
            "vignetting_function_path": os.path.join(best_vignetting_function.directory(pipeline_config['root']),
                                                     best_vignetting_function.filename()),
            "psf_model_path": os.path.join(best_psf_model.directory(pipeline_config['root']),
                                           best_psf_model.filename()),
            "quartic_coefficient_path": os.path.join(best_quartic_model.directory(pipeline_config['root']),
                                                     best_quartic_model.filename()),
            "gain_bottom": ccd_parameters['gain_bottom'],
            "gain_top": ccd_parameters['gain_top'],
            "distortion_path": os.path.join(best_distortion.directory(pipeline_config['root']),
                                            best_distortion.filename()),
            "stray_light_path": os.path.join(best_stray_light.directory(pipeline_config['root']),
                                            best_stray_light.filename()),
            "mask_path": os.path.join(mask_function.directory(pipeline_config['root']),
                                      mask_function.filename().replace('.fits', '.bin')),
            "return_with_stray_light": True,
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


def level1_construct_file_info(level0_files: t.List[File], pipeline_config: dict, reference_time=None) -> t.List[File]:
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
        ),
        File(
            level="1",
            file_type='X' + level0_files[0].file_type[1:],
            observatory=level0_files[0].observatory,
            file_version=pipeline_config["file_version"],
            software_version=__version__,
            date_obs=level0_files[0].date_obs,
            polarization=level0_files[0].polarization,
            state="planned",
        )
    ]


@flow
def level1_scheduler_flow(pipeline_config_path=None, session=None, reference_time=None):
    generic_scheduler_flow_logic(
        level1_query_ready_files,
        level1_construct_file_info,
        level1_construct_flow_info,
        pipeline_config_path,
        reference_time=reference_time,
        session=session,
    )


def level1_call_data_processor(call_data: dict, pipeline_config=None, session=None) -> dict:
    call_data['psf_model_path'] = cache_layer.psf.wrap_if_appropriate(call_data['psf_model_path'])
    call_data['quartic_coefficient_path'] = cache_layer.quartic_coefficients.wrap_if_appropriate(
        call_data['quartic_coefficient_path'])
    call_data['vignetting_function_path'] = cache_layer.vignetting_function.wrap_if_appropriate(
        call_data['vignetting_function_path'])
    # Anything more than 16 doesn't offer any real benefit, and the default of n_cpu on punch190 is actually slower than
    # 16! Here we choose less to have less spiky CPU usage to play better with other flows.
    call_data['max_workers'] = 2
    return call_data


@flow
def level1_process_flow(flow_id: int, pipeline_config_path=None, session=None):
    generic_process_flow_logic(flow_id, level1_core_flow, pipeline_config_path, session=session,
                               call_data_processor=level1_call_data_processor)
