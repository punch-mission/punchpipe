import os
import json
import random
import typing as t
from datetime import datetime, UTC, timedelta
from functools import partial

from prefect import flow, get_run_logger, task
from punchbowl.level2.flow import levelq_core_flow
from punchbowl.levelq.f_corona_model import construct_qp_f_corona_model

from punchpipe import __version__
from punchpipe.control.db import File, Flow
from punchpipe.control.processor import generic_process_flow_logic
from punchpipe.control.scheduler import generic_scheduler_flow_logic


@task
def levelq_query_ready_files(session, pipeline_config: dict, reference_time=None):
    logger = get_run_logger()
    all_ready_files = (session.query(File).filter(File.state == "created")
                       .filter(File.level == "1")
                       .filter(File.file_type == "CR").all())
    logger.info(f"{len(all_ready_files)} ready files")
    unique_times = set(f.date_obs for f in all_ready_files)
    logger.info(f"{len(unique_times)} unique times: {unique_times}")
    grouped_ready_files = [[f.file_id for f in all_ready_files if f.date_obs == time] for time in unique_times]
    logger.info(f"{len(grouped_ready_files)} grouped ready files")
    out = [g for g in grouped_ready_files if len(g) == 4]
    logger.info(f"{len(out)} groups heading out")
    return out


@task
def levelq_construct_flow_info(level1_files: list[File], levelq_file: File, pipeline_config: dict, session=None, reference_time=None):
    flow_type = "levelq"
    state = "planned"
    creation_time = datetime.now(UTC)
    priority = pipeline_config["flows"][flow_type]["priority"]["initial"]
    call_data = json.dumps(
        {
            "data_list": [
                os.path.join(level1_file.directory(pipeline_config["root"]), level1_file.filename())
                for level1_file in level1_files
            ]
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
def levelq_construct_file_info(level1_files: t.List[File], pipeline_config: dict, reference_time=None) -> t.List[File]:
    return [File(
                level="Q",
                file_type="CT",
                observatory="M",
                file_version=pipeline_config["file_version"],
                software_version=__version__,
                date_obs=level1_files[0].date_obs,
                state="planned",
            ),
            File(
                level="Q",
                file_type="CN",
                observatory="N",
                file_version=pipeline_config["file_version"],
                software_version=__version__,
                date_obs=level1_files[0].date_obs,
                state="planned",
            )
    ]


@flow
def levelq_scheduler_flow(pipeline_config_path=None, session=None, reference_time=None):
    generic_scheduler_flow_logic(
        levelq_query_ready_files,
        levelq_construct_file_info,
        levelq_construct_flow_info,
        pipeline_config_path,
        reference_time=reference_time,
        session=session,
    )


@flow
def levelq_process_flow(flow_id: int, pipeline_config_path=None, session=None):
    generic_process_flow_logic(flow_id, levelq_core_flow, pipeline_config_path, session=session)


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
    creation_time = datetime.now(UTC)
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
    for file_name in data_list:
        os.system(f"aws --profile {aws_profile} s3 cp {file_name} {bucket_name}")

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
    creation_time = datetime.now(UTC)
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
    creation_time = datetime.now(UTC)
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
