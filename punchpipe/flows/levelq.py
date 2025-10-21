import os
import json
import random
import typing as t
from datetime import UTC, datetime, timedelta
from functools import partial

import numpy as np
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from prefect.context import get_run_context
from punchbowl.levelq.f_corona_model import construct_qp_f_corona_model
from punchbowl.levelq.flow import levelq_CNN_core_flow, levelq_CTM_core_flow
from punchbowl.util import average_datetime
from sqlalchemy import and_, func, or_, select, text

from punchpipe import __version__
from punchpipe.control.cache_layer.nfi_l1 import wrap_if_appropriate
from punchpipe.control.db import File, Flow
from punchpipe.control.processor import generic_process_flow_logic
from punchpipe.control.scheduler import generic_scheduler_flow_logic
from punchpipe.control.util import get_database_session, group_files_by_time, load_pipeline_configuration
from punchpipe.flows.util import file_name_to_full_path


@task(cache_policy=NO_CACHE)
def levelq_CNN_query_ready_files(session, pipeline_config: dict, reference_time=None, max_n=9e99):
    logger = get_run_logger()
    pending_flows = session.query(Flow).filter(Flow.flow_type == "levelq_CNN").filter(Flow.state == "planned").all()
    if pending_flows:
        logger.info("A pending flow already exists. Skipping scheduling to let the batch grow.")

    all_fittable_files = (session.query(File).filter(File.state.in_(("created", "progressed")))
                          .filter(File.level == "1")
                          .filter(File.observatory == "4")
                          .filter(~File.outlier)
                          .filter(File.file_type == "QR").limit(1000).all())
    if len(all_fittable_files) < 1000:
        logger.info("Not enough fittable files")
        return []
    all_ready_files = (session.query(File).filter(File.state == "created")
                       .filter(File.level == "1")
                       .filter(File.observatory == "4")
                       .filter(File.file_type == "QR").order_by(File.date_obs.desc()).limit(1000).all())
    logger.info(f"{len(all_ready_files)} ready files")

    if len(all_ready_files) == 0:
        return []

    # We want a batch of lots of files, but we probably don't want them spread too far in time, so let's group these
    # files up with a maximum time span, and take just the first group.
    grouped_files = group_files_by_time(all_ready_files, max_duration_seconds=60*60*24*15, max_per_group=1000)
    grouped_files = grouped_files[0]

    # Let's order it oldest-to-newest. They're currently the opposite from the database's sort
    grouped_files = grouped_files[::-1]
    logger.info("1 group heading out")
    return [grouped_files]


@task(cache_policy=NO_CACHE)
def levelq_CNN_construct_flow_info(level1_files: list[File], levelq_file: File, pipeline_config: dict, session=None, reference_time=None):
    flow_type = "levelq_CNN"
    state = "planned"
    creation_time = datetime.now()
    priority = pipeline_config["flows"][flow_type]["priority"]["initial"]
    call_data = json.dumps(
        {
            "data_list": [level1_file.filename() for level1_file in level1_files],
            # This date_obs is only used to find other files to fit the PCA to, if there aren't enough
            # to-be-subtracted images in the batch
            "date_obs": average_datetime([f.date_obs for f in level1_files]).strftime("%Y-%m-%d %H:%M:%S"),
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
                polarization="C",
                file_version=pipeline_config["file_version"],
                software_version=__version__,
                date_obs=level1_file.date_obs,
                state="planned",
                outlier=level1_file.outlier,
                bad_packets=level1_file.bad_packets,
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
        children_are_one_to_one=True,
    )


def levelq_CNN_call_data_processor(call_data: dict, pipeline_config, session) -> dict:
    # Prepend the data root to each input file
    for key in ['data_list']:
        if call_data[key] is not None:
            call_data[key] = file_name_to_full_path(call_data[key], pipeline_config['root'])

    # How many files we want for the PCA fitting
    target_number = 1100
    files_to_fit = session.execute(
        select(File,
               dt := func.abs(func.timestampdiff(text("second"), File.date_obs, call_data['date_obs'])))
        .filter(File.state.in_(("created", "progressed")))
        .filter(File.level == "1")
        .filter(File.file_type == "QR")
        .filter(File.observatory == "4")
        .filter(~File.outlier)
        .filter(dt > 10 * 60)
        .order_by(dt.asc()).limit(target_number)).all()

    files_to_fit = [os.path.join(f.directory(pipeline_config['root']), f.filename()) for f, _ in files_to_fit]

    # Remove files that we're subtracting
    files_to_fit = [f for f in files_to_fit if f not in call_data['data_list']]
    # Figure out how many of these extra files we need to meet our target number for fitting
    n_to_use = target_number - len(call_data['data_list'])
    n_to_use = max(0, n_to_use)
    files_to_fit = files_to_fit[:n_to_use]
    files_to_fit = [wrap_if_appropriate(f) for f in files_to_fit]

    call_data['files_to_fit'] = files_to_fit
    del call_data['date_obs']
    return call_data


@flow
def levelq_CNN_process_flow(flow_id: int | list[int], pipeline_config_path=None, session=None):
    generic_process_flow_logic(flow_id, levelq_CNN_core_flow, pipeline_config_path, session=session,
                               call_data_processor=levelq_CNN_call_data_processor)


@task(cache_policy=NO_CACHE)
def levelq_CTM_query_ready_files(session, pipeline_config: dict, reference_time=None, max_n=9e99):
    logger = get_run_logger()
    all_ready_files = (session.query(File).filter(File.state == "created")
                       .filter(or_(
                            and_(File.level == "1", File.file_type == "QR", File.observatory.in_(['1', '2', '3'])),
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
        if len(grouped_ready_files) >= max_n:
            break
        # TODO: We're excluding NFI for now
        # group_is_complete = len(group) == 4
        group_is_complete = len(group) == 3
        if group_is_complete:
            grouped_ready_files.append(group)
            continue

        # group[-1] is the newest file by date_obs
        if (cutoff_time and group[-1].date_obs.replace(tzinfo=UTC) > cutoff_time):
            # We're still potentially waiting for downlinks
            continue


        # We now have to consider making an incomplete trefoil. We want to look at the L0 files to see if we're still
        # waiting on any L1s. This is especially important when reprocessing. To do that, we need to determine a time
        # range within which to grab L0s
        center = group[0].date_obs
        search_width = timedelta(minutes=1)
        search_types = ['QR']

        # Grab all the L0s that produce inputs for this trefoil
        expected_inputs = (session.query(File)
                                  .filter(File.level == "0")
                                  # TODO: This line temporarily excludes NFI
                                  .filter(File.observatory.in_(['1', '2', '3']))
                                  .filter(File.file_type.in_(search_types))
                                  .filter(File.date_obs > center - search_width)
                                  .filter(File.date_obs < center + search_width)
                                  .all())
        if len(expected_inputs) == len(group):
            # We have the L1s for all the L0s, and we don't expect new L0s, so let's make an incomplete mosaic
            grouped_ready_files.append(group)
        # Otherwise, we'll pass for now on processing this trefoil
        continue

    logger.info(f"{len(grouped_ready_files)} groups heading out")
    return grouped_ready_files


@task(cache_policy=NO_CACHE)
def levelq_CTM_construct_flow_info(level1_files: list[File], levelq_file: File, pipeline_config: dict, session=None, reference_time=None):
    flow_type = "levelq_CTM"
    state = "planned"
    creation_time = datetime.now()
    priority = pipeline_config["flows"][flow_type]["priority"]["initial"]
    alphas_path = pipeline_config["flows"][flow_type].get("alpha_file_path", None)
    trim_edges_px = pipeline_config["flows"][flow_type].get("trim_edges_px", 0)
    call_data = json.dumps(
        {
            "data_list": [level1_file.filename() for level1_file in level1_files],
            "alphas_file": alphas_path,
            "trim_edges_px": trim_edges_px,
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
                polarization="C",
                file_version=pipeline_config["file_version"],
                software_version=__version__,
                date_obs=average_datetime([f.date_obs for f in level1_files]),
                state="planned",
                outlier=any(file.outlier for file in level1_files),
                bad_packets=any(file.bad_packets for file in level1_files),
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
    )


def levelq_CTM_call_data_processor(call_data: dict, pipeline_config, session=None) -> dict:
    call_data['data_list'] = file_name_to_full_path(call_data['data_list'], pipeline_config['root'])
    return call_data


@flow
def levelq_CTM_process_flow(flow_id: int | list[int], pipeline_config_path=None, session=None):
    generic_process_flow_logic(flow_id, levelq_CTM_core_flow, pipeline_config_path, session=session,
                               call_data_processor=levelq_CTM_call_data_processor)


@task
def levelq_upload_query_ready_files(session, pipeline_config: dict, reference_time=None):
    logger = get_run_logger()
    lookback_days = pipeline_config['flows']["levelq_upload"].get("lookback_days", np.inf)
    if np.isfinite(lookback_days):
        all_ready_files = (session.query(File).filter(File.state == "created")
                           .filter(File.level == "Q")
                           .filter(File.date_obs >= datetime.now(UTC) - timedelta(days=lookback_days)).all())
    else:
        all_ready_files = (session.query(File).filter(File.state == "created")
                           .filter(File.level == "Q").all())
    logger.info(f"{len(all_ready_files)} ready files")
    currently_creating_files = session.query(File).filter(File.state == "creating").filter(File.level == "Q").all()
    logger.info(f"{len(currently_creating_files)} level Q files currently being processed")
    out = [f.file_id for f in all_ready_files]
    logger.info(f"Delivering {len(out)} level Q files in this batch.")
    return [out]

@task
def levelq_upload_construct_flow_info(levelq_files: list[File], intentionally_empty: File, pipeline_config: dict, session=None, reference_time=None):
    flow_type = "levelq_upload"
    state = "planned"
    creation_time = datetime.now()
    priority = pipeline_config["flows"][flow_type]["priority"]["initial"]
    call_data = json.dumps(
        {
            "data_list": [levelq_file.filename() for levelq_file in levelq_files],
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
    logger = get_run_logger()
    if session is None:
        session = get_database_session()
    pipeline_config = load_pipeline_configuration(pipeline_config_path)
    # fetch the appropriate flow db entry
    flow_db_entry = session.query(Flow).where(Flow.flow_id == flow_id).one()
    logger.info(f"Running on flow db entry with id={flow_db_entry.flow_id}.")

    # update the processing flow name with the flow run name from Prefect
    flow_run_context = get_run_context()
    flow_db_entry.flow_run_name = flow_run_context.flow_run.name
    flow_db_entry.flow_run_id = flow_run_context.flow_run.id
    flow_db_entry.state = "running"
    flow_db_entry.start_time = datetime.now(UTC)
    session.commit()

    # load the call data and launch the core flow
    flow_call_data = json.loads(flow_db_entry.call_data)
    logger.info(f"Running with {flow_call_data}")

    flow_call_data['data_list'] = file_name_to_full_path(flow_call_data['data_list'], pipeline_config['root'])

    try:
        levelq_upload_core_flow(**flow_call_data)
    except Exception as e:
        flow_db_entry.state = "failed"
        flow_db_entry.end_time = datetime.now(UTC)
        logger.info("Something's gone wrong - level0_core_flow failed")
        session.commit()
        raise e
    else:
        flow_db_entry.state = "completed"
        flow_db_entry.end_time = datetime.now(UTC)
        # Note: the file_db_entry gets updated above in the writing step because it could be created or blank
        session.commit()

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
            "filenames": [ctm_file.filename() for ctm_file in levelq_CTM_files],
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


def levelq_CFM_call_data_processor(call_data: dict, pipeline_config, session=None) -> dict:
    call_data['filenames'] = file_name_to_full_path(call_data['filenames'], pipeline_config['root'])
    return call_data

@flow
def levelq_CFM_process_flow(flow_id: int | list[int], pipeline_config_path=None, session=None):
    generic_process_flow_logic(flow_id, partial(construct_qp_f_corona_model, product_code="CFM"),
                               pipeline_config_path, session=session,
                               call_data_processor=levelq_CFM_call_data_processor)

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
            "filenames": [cnn_file.filename() for cnn_file in levelq_CNN_files],
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


def levelq_CFN_call_data_processor(call_data: dict, pipeline_config, session=None) -> dict:
    call_data['filenames'] = file_name_to_full_path(call_data['filenames'], pipeline_config['root'])
    return call_data

@flow
def levelq_CFN_process_flow(flow_id: int | list[int], pipeline_config_path=None, session=None):
    generic_process_flow_logic(flow_id, partial(construct_qp_f_corona_model, product_code="CFN"),
                               pipeline_config_path, session=session,
                               call_data_processor=levelq_CFN_call_data_processor)
