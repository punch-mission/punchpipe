import os
import json
import typing as t
from datetime import datetime, timedelta

from prefect import flow, get_run_logger, task
from punchbowl.data.meta import construct_all_product_codes
from punchbowl.data.punch_io import write_jp2_to_mp4

from punchpipe import __version__
from punchpipe.control.db import File, Flow
from punchpipe.control.processor import generic_process_flow_logic
from punchpipe.control.scheduler import generic_scheduler_flow_logic


@task
def visualize_query_ready_files(session, pipeline_config: dict, reference_time: datetime):
    logger = get_run_logger()

    all_ready_files = []
    all_ready_levels = []
    all_ready_codes = []
    levels = ["0", "1", "2", "3", "Q", "L"]
    for level in levels:
        product_codes = construct_all_product_codes(level=level)
        for product_code in product_codes:
            # TODO - How to filter out only jp2 files from the db? Find FITS files, just change the extension to jp2
            product_ready_files = (session.query(File)
                                    .filter(File.state.in_(["created", "progressed"]))
                                    .filter(File.date_obs >= (reference_time - timedelta(hours=24)))
                                    .filter(File.date_obs <= reference_time)
                                    .filter(File.level == level)
                                    .filter(File.file_type == product_code[0:2])
                                    .filter(File.observatory == product_code[2]).all())
            all_ready_files.append(list(product_ready_files))
            all_ready_codes.append(product_codes)
            all_ready_levels.append([level] * len(product_codes))

    logger.info(f"{len(all_ready_files)} files will be used for visualization.")
    return [[f.file_id for f in all_ready_files]], all_ready_codes, all_ready_levels


@task
def visualize_flow_info(input_files: list[File],
                        level3_f_model_file: File,
                        pipeline_config: dict,
                        reference_time: datetime,
                        session=None
                        ):
    flow_type = "visualize_process_flow"
    state = "planned"
    creation_time = datetime.now()
    priority = pipeline_config["levels"][flow_type]["priority"]["initial"]
    call_data = json.dumps(
        {
            "filenames": [
                os.path.join(input_file.directory(pipeline_config["root"]), input_file.filename())
                for input_file in input_files
            ],
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
def visualize_file_info(input_files: t.List[File],
                        output_level: str,
                        output_code: str,
                        pipeline_config: dict,
                        reference_time: datetime) -> t.List[File]:
    # TODO - need to get output details passed from query function output
    return [File(
                level=output_level,
                file_type=output_code[0:2],
                observatory=output_code[2],
                file_version=pipeline_config["file_version"],
                software_version=__version__,
                date_obs= reference_time,
                state="planned",
            ),]


@flow
def visualize_scheduler(pipeline_config_path=None, session=None, reference_time: datetime | None = None):
    reference_time = reference_time or datetime.now()

    generic_scheduler_flow_logic(
        visualize_query_ready_files,
        visualize_file_info,
        visualize_flow_info,
        pipeline_config_path,
        update_input_file_state=True,
        new_input_file_state="visualized",
        reference_time=reference_time,
        session=session,
    )


# TODO - make a helper function to iterate over list of list of file inputs and list of outputs to call the write_jp2 function each time
@flow
def movie_process(flow_id: int, pipeline_config_path=None, session=None):
    generic_process_flow_logic(flow_id, write_jp2_to_mp4, pipeline_config_path, session=session)
