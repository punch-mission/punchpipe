import os
import json
import random
import typing as t
from datetime import UTC, datetime, timedelta
from functools import partial

from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from punchbowl.levelq.f_corona_model import construct_qp_f_corona_model
from punchbowl.levelq.flow import levelq_CNN_core_flow, levelq_CTM_core_flow
from punchbowl.util import average_datetime
from sqlalchemy import and_, func, or_, select, text

from punchpipe import __version__
from punchpipe.control.cache_layer.nfi_l1 import wrap_if_appropriate
from punchpipe.control.db import File, Flow
from punchpipe.control.processor import generic_process_flow_logic
from punchpipe.control.scheduler import generic_scheduler_flow_logic
from punchpipe.control.util import group_files_by_time


@task(cache_policy=NO_CACHE)
def levelq_CNN_query_ready_files(session, pipeline_config: dict, reference_time=None, max_n=9e99):
    logger = get_run_logger()
    all_fittable_files = (session.query(File).filter(File.state.in_(("created", "quickpunched", "progressed")))
                          .filter(File.level == "1")
                          .filter(File.observatory == "4")
                          .filter(File.file_type == "CR").limit(1000).all())
    if len(all_fittable_files) < 1000:
        logger.info("Not enough fittable files")
        return []
    all_ready_files = (session.query(File).filter(File.state == "created")
                       .filter(File.level == "1")
                       .filter(File.observatory == "4")
                       .filter(File.file_type == "CR").order_by(File.date_obs.desc()).limit(1000).all())
    logger.info(f"{len(all_ready_files)} ready files")

    if len(all_ready_files) == 0:
        return []

    grouped_files = group_files_by_time(all_ready_files, max_duration_seconds=60*60*24*15, max_per_group=1000)
    # Let's take just the first group
    grouped_files = grouped_files[0]

    # Let's order it oldest-to-newest. They're currently the opposite from the database's sort
    grouped_files = grouped_files[::-1]
    logger.info(f"1 group heading out")
    return [[f.file_id for f in grouped_files]]


def get_outlier_limits_path(level1_file, pipeline_config: dict=None, session=None, reference_time=None):
    corresponding_outlier_limits_type = {"PM": "LM",
                                         "PZ": "LZ",
                                         "PP": "LP",
                                         "CR": "LR"}
    outlier_limits_type = corresponding_outlier_limits_type[level1_file.file_type]
    best_limits = (session.query(File)
                     .filter(File.file_type == outlier_limits_type)
                     .filter(File.observatory == level1_file.observatory)
                     .where(File.date_obs <= level1_file.date_obs)
                     .order_by(File.date_obs.desc(), File.file_version.desc()).first())
    return best_limits


@task(cache_policy=NO_CACHE)
def levelq_CNN_construct_flow_info(level1_files: list[File], levelq_file: File, pipeline_config: dict, session=None, reference_time=None):
    flow_type = "levelq_CNN"
    state = "planned"
    creation_time = datetime.now()
    priority = pipeline_config["flows"][flow_type]["priority"]["initial"]
    outlier_limits = get_outlier_limits_path(level1_files[0], session=session)
    if outlier_limits is not None:
        outlier_limits = os.path.join(outlier_limits.directory(pipeline_config['root']),
                                      outlier_limits.filename().replace('.fits', '.npz'))
    call_data = json.dumps(
        {
            # We need to send the data root separately, rather than prepended to each input file, because otherwise we
            # risk generating too much data for the database column that holds it.
            "data_root": pipeline_config["root"],
            "data_list": [
                os.path.join(level1_file.directory(''), level1_file.filename())
                for level1_file in level1_files
            ],
            # This date_obs is only used to find other files to fit the PCA to
            "date_obs": average_datetime([f.date_obs for f in level1_files]).strftime("%Y-%m-%d %H:%M:%S"),
            "outlier_limits": outlier_limits,
        }
    )
    return Flow(
        flow_type=flow_type,
        state=state,
        flow_level="Q",
        creation_time=creation_time,
        priority=priority,
        call_data=call_data,
    )


@task
def levelq_CNN_construct_file_info(level1_files: t.List[File], pipeline_config: dict, reference_time=None) -> t.List[File]:
    return [File(
                level="Q",
                file_type="CN",
                observatory="N",
                file_version=pipeline_config["file_version"],
                software_version=__version__,
                date_obs=level1_file.date_obs,
                state="planned",
            )
        for level1_file in level1_files
    ]


@flow
def levelq_CNN_scheduler_flow(pipeline_config_path=None, session=None, reference_time=None):
    generic_scheduler_flow_logic(
        levelq_CNN_query_ready_files,
        levelq_CNN_construct_file_info,
        levelq_CNN_construct_flow_info,
        pipeline_config_path,
        reference_time=reference_time,
        session=session,
        new_input_file_state="quickpunched"
    )


def levelq_CNN_call_data_processor(call_data: dict, pipeline_config, session) -> dict:
    target_number = 1100
    files_to_fit = session.execute(
        select(File,
               dt := func.abs(func.timestampdiff(text("second"), File.date_obs, call_data['date_obs'])))
        .filter(File.state.in_(("created", "quickpunched", "progressed")))
        .filter(File.level == "1")
        .filter(File.file_type == "CR")
        .filter(File.observatory == "4")
        .filter(dt > 10 * 60)
        .order_by(dt.asc()).limit(target_number)).all()

    call_data['data_list'] = [os.path.join(call_data['data_root'], f) for f in call_data['data_list']]

    files_to_fit = [os.path.join(f.directory(call_data['data_root']), f.filename()) for f, _ in files_to_fit]

    files_to_fit = [f for f in files_to_fit if f not in call_data['data_list']]
    files_to_fit = [wrap_if_appropriate(f) for f in files_to_fit]

    call_data['files_to_fit'] = files_to_fit
    del call_data['date_obs']
    return call_data


@flow
def levelq_CNN_process_flow(flow_id: int, pipeline_config_path=None, session=None):
    generic_process_flow_logic(flow_id, levelq_CNN_core_flow, pipeline_config_path, session=session,
                               call_data_processor=levelq_CNN_call_data_processor)


@task(cache_policy=NO_CACHE)
def levelq_CTM_query_ready_files(session, pipeline_config: dict, reference_time=None, max_n=9e99):
    logger = get_run_logger()
    all_ready_files = (session.query(File).filter(File.state == "created")
                       .filter(or_(
                            and_(File.level == "1", File.file_type == "CR", File.observatory.in_(['1', '2', '3'])),
                            # TODO: We're excluding NFI for now
                            # and_(File.level == "Q", File.file_type == "CN"),
                       )).order_by(File.date_obs.desc()).all())
    logger.info(f"{len(all_ready_files)} ready files")

    if len(all_ready_files) == 0:
        return []

    grouped_files = group_files_by_time(all_ready_files, max_duration_seconds=10)

    logger.info(f"{len(grouped_files)} unique times")
    grouped_ready_files = []
    cutoff_time = pipeline_config["flows"]["levelq_CTM"].get("ignore_missing_after_days", None)
    if cutoff_time is not None:
        cutoff_time = datetime.now(tz=UTC) - timedelta(days=cutoff_time)
    for group in grouped_files:
        # TODO: We're excluding NFI for now
        # if len(group) == 4 or group[-1].date_obs.replace(tzinfo=UTC) < cutoff_time:
        if len(group) == 3 or (cutoff_time and group[-1].date_obs.replace(tzinfo=UTC) < cutoff_time):
            grouped_ready_files.append([f.file_id for f in group])
        if len(grouped_ready_files) >= max_n:
            break
    logger.info(f"{len(grouped_ready_files)} groups heading out")
    return grouped_ready_files


@task(cache_policy=NO_CACHE)
def levelq_CTM_construct_flow_info(level1_files: list[File], levelq_file: File, pipeline_config: dict, session=None, reference_time=None):
    flow_type = "levelq_CTM"
    state = "planned"
    creation_time = datetime.now()
    priority = pipeline_config["flows"][flow_type]["priority"]["initial"]
    call_data = json.dumps(
        {
            "data_list": [
                os.path.join(level1_file.directory(pipeline_config["root"]), level1_file.filename())
                for level1_file in level1_files
            ],
        }
    )
    return Flow(
        flow_type=flow_type,
        state=state,
        flow_level="Q",
        creation_time=creation_time,
        priority=priority,
        call_data=call_data,
    )


@task
def levelq_CTM_construct_file_info(level1_files: t.List[File], pipeline_config: dict, reference_time=None) -> t.List[File]:
    return [File(
                level="Q",
                file_type="CT",
                observatory="M",
                file_version=pipeline_config["file_version"],
                software_version=__version__,
                date_obs=average_datetime([f.date_obs for f in level1_files]),
                state="planned",
            ),
    ]


@flow
def levelq_CTM_scheduler_flow(pipeline_config_path=None, session=None, reference_time=None):
    generic_scheduler_flow_logic(
        levelq_CTM_query_ready_files,
        levelq_CTM_construct_file_info,
        levelq_CTM_construct_flow_info,
        pipeline_config_path,
        reference_time=reference_time,
        session=session,
        new_input_file_state="quickpunched"
    )


@flow
def levelq_CTM_process_flow(flow_id: int, pipeline_config_path=None, session=None):
    generic_process_flow_logic(flow_id, levelq_CTM_core_flow, pipeline_config_path, session=session)


@task
def levelq_upload_query_ready_files(session, pipeline_config: dict, reference_time=None):
    logger = get_run_logger()
    all_ready_files = (session.query(File).filter(File.state == "created")
                       .filter(File.level == "Q").all())
    logger.info(f"{len(all_ready_files)} ready files")
    currently_creating_files = session.query(File).filter(File.state == "creating").filter(File.level == "Q").all()
    logger.info(f"{len(currently_creating_files)} level Q files currently being processed")
    out = all_ready_files if len(currently_creating_files) == 0 else []
    logger.info(f"Delivering {len(out)} level Q files in this batch.")
    return out

@task
def levelq_upload_construct_flow_info(levelq_files: list[File], intentionally_empty: File, pipeline_config: dict, session=None, reference_time=None):
    flow_type = "levelQ_upload"
    state = "planned"
    creation_time = datetime.now()
    priority = pipeline_config["flows"][flow_type]["priority"]["initial"]
    call_data = json.dumps(
        {
            "data_list": [
                os.path.join(levelq_file.directory(pipeline_config["root"]), levelq_file.filename())
                for levelq_file in levelq_files
            ],
            "bucket_name": pipeline_config["bucket_name"],
        }
    )
    return Flow(
        flow_type=flow_type,
        state=state,
        flow_level="Q",
        creation_time=creation_time,
        priority=priority,
        call_data=call_data,
    )


@task
def levelq_upload_construct_file_info(level1_files: t.List[File], pipeline_config: dict, reference_time=None) -> t.List[File]:
    return []

@flow
def levelq_upload_scheduler_flow(pipeline_config_path=None, session=None, reference_time=None):
    generic_scheduler_flow_logic(
        levelq_upload_query_ready_files,
        levelq_upload_construct_file_info,
        levelq_upload_construct_flow_info,
        pipeline_config_path,
        reference_time=reference_time,
        session=session,
    )

@flow
def levelq_upload_core_flow(data_list, bucket_name, aws_profile="noaa-prod"):
    data_list += [fn + '.sha' for fn in data_list]
    manifest_path = write_manifest(data_list)
    os.system(f"aws --profile {aws_profile} s3 cp {manifest_path} {bucket_name}")
    for file_name in data_list:
        os.system(f"aws --profile {aws_profile} s3 cp {file_name} {bucket_name}")


def write_manifest(file_names):
    now = datetime.now(UTC)
    stamp = now.strftime("%Y%m%d%H%M%S")
    manifest_name = os.path.join('/mnt/archive/soc/data/noaa_manifests', f"PUNCH_LQ_manifest_{stamp}.txt")
    with open(manifest_name, "w") as f:
        f.write("\n".join([os.path.basename(fn) for fn in file_names]))
    return manifest_name

@flow
def levelq_upload_process_flow(flow_id, pipeline_config_path=None, session=None):
    generic_process_flow_logic(flow_id, levelq_upload_core_flow, pipeline_config_path, session=session)


@task
def levelq_CFM_query_ready_files(session, pipeline_config: dict, reference_time: datetime, use_n: int = 50):
    before = reference_time - timedelta(weeks=4)
    after = reference_time + timedelta(weeks=0)

    logger = get_run_logger()
    all_ready_files = (session.query(File)
                       .filter(File.state.in_(["created", "progressed"]))
                       .filter(File.date_obs >= before)
                       .filter(File.date_obs <= after)
                       .filter(File.level == "Q")
                       .filter(File.file_type == "CT")
                       .filter(File.observatory == "M").all())
    logger.info(f"{len(all_ready_files)} Level Q CTM files will be used for F corona background modeling.")
    if len(all_ready_files) > 30:  #  need at least 30 images
        random.shuffle(all_ready_files)
        return [[f.file_id for f in all_ready_files[:use_n]]]
    else:
        return []

@task
def construct_levelq_CFM_flow_info(levelq_CTM_files: list[File],
                                            levelq_CFM_model_file: File,
                                            pipeline_config: dict,
                                            reference_time: datetime,
                                            session=None
                                            ):
    flow_type = "levelQ_CFM"
    state = "planned"
    creation_time = datetime.now()
    priority = pipeline_config["flows"][flow_type]["priority"]["initial"]
    call_data = json.dumps(
        {
            "filenames": [
                os.path.join(ctm_file.directory(pipeline_config["root"]), ctm_file.filename())
                for ctm_file in levelq_CTM_files
            ],
            "reference_time": str(reference_time)
        }
    )
    return Flow(
        flow_type=flow_type,
        state=state,
        flow_level="Q",
        creation_time=creation_time,
        priority=priority,
        call_data=call_data,
    )


@task
def construct_levelq_CFM_background_file_info(levelq_files: t.List[File], pipeline_config: dict,
                                            reference_time: datetime) -> t.List[File]:
    return [File(
                level="Q",
                file_type="CF",
                observatory="M",
                file_version=pipeline_config["file_version"],
                software_version=__version__,
                date_obs= reference_time,
                state="planned",
            ),]

@flow
def levelq_CFM_scheduler_flow(pipeline_config_path=None, session=None, reference_time=None):
    reference_time = reference_time or datetime.now(UTC)

    generic_scheduler_flow_logic(
        levelq_CFM_query_ready_files,
        construct_levelq_CFM_background_file_info,
        construct_levelq_CFM_flow_info,
        pipeline_config_path,
        update_input_file_state=False,
        reference_time=reference_time,
        session=session,
    )

@flow
def levelq_CFM_process_flow(flow_id, pipeline_config_path=None, session=None):
    generic_process_flow_logic(flow_id, partial(construct_qp_f_corona_model, product_code="CFM"),
                               pipeline_config_path, session=session)

@task
def levelq_CFN_query_ready_files(session, pipeline_config: dict, reference_time: datetime, use_n: int = 50):
    before = reference_time - timedelta(weeks=4)
    after = reference_time + timedelta(weeks=0)

    logger = get_run_logger()
    all_ready_files = (session.query(File)
                       .filter(File.state.in_(["created", "progressed"]))
                       .filter(File.date_obs >= before)
                       .filter(File.date_obs <= after)
                       .filter(File.level == "Q")
                       .filter(File.file_type == "CN")
                       .filter(File.observatory == "N").all())
    logger.info(f"{len(all_ready_files)} Level Q CNN files will be used for F corona background modeling.")
    if len(all_ready_files) > 30:  #  need at least 30 images
        random.shuffle(all_ready_files)
        return [[f.file_id for f in all_ready_files[:use_n]]]
    else:
        return []

@task
def construct_levelq_CFN_flow_info(levelq_CNN_files: list[File],
                                            levelq_CFN_model_file: File,
                                            pipeline_config: dict,
                                            reference_time: datetime,
                                            session=None
                                            ):
    flow_type = "levelQ_CFN"
    state = "planned"
    creation_time = datetime.now()
    priority = pipeline_config["flows"][flow_type]["priority"]["initial"]
    call_data = json.dumps(
        {
            "filenames": [
                os.path.join(cnn_file.directory(pipeline_config["root"]), cnn_file.filename())
                for cnn_file in levelq_CNN_files
            ],
            "reference_time": str(reference_time)
        }
    )
    return Flow(
        flow_type=flow_type,
        state=state,
        flow_level="Q",
        creation_time=creation_time,
        priority=priority,
        call_data=call_data,
    )


@task
def construct_levelq_CFN_background_file_info(levelq_files: t.List[File], pipeline_config: dict,
                                            reference_time: datetime) -> t.List[File]:
    return [File(
                level="Q",
                file_type="CF",
                observatory="N",
                file_version=pipeline_config["file_version"],
                software_version=__version__,
                date_obs= reference_time,
                state="planned",
            ),]

@flow
def levelq_CFN_scheduler_flow(pipeline_config_path=None, session=None, reference_time=None):
    reference_time = reference_time or datetime.now(UTC)

    generic_scheduler_flow_logic(
        levelq_CFN_query_ready_files,
        construct_levelq_CFN_background_file_info,
        construct_levelq_CFN_flow_info,
        pipeline_config_path,
        update_input_file_state=False,
        reference_time=reference_time,
        session=session,
    )

@flow
def levelq_CFN_process_flow(flow_id, pipeline_config_path=None, session=None):
    generic_process_flow_logic(flow_id, partial(construct_qp_f_corona_model, product_code="CFN"),
                               pipeline_config_path, session=session)
