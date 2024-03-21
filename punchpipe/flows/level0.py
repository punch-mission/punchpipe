import os
from glob import glob

from prefect import flow

from punchpipe.controlsegment.scheduler import generic_scheduler_flow_logic


def level0_query_ready_files(session, pipeline_config: dict):
    dropzone = os.path.join(pipeline_config["root"], pipeline_config["input_drop"])
    return glob(os.path.join(dropzone, "*.tlm"))


def level0_construct_file_info():
    pass


def level0_construct_flow_info():
    pass


@flow
def level0_scheduler_flow(pipeline_config_path="config.yaml", session=None):
    generic_scheduler_flow_logic(
        level0_query_ready_files,
        level0_construct_file_info,
        level0_construct_flow_info,
        pipeline_config_path,
        session=session,
    )
