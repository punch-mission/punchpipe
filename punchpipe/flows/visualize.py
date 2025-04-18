import os
import json
import tempfile
from datetime import UTC, datetime, timedelta

from prefect import flow, get_run_logger, task
from prefect.context import get_run_context
from punchbowl.data.meta import construct_all_product_codes
from punchbowl.data.punch_io import load_ndcube_from_fits, write_ndcube_to_quicklook, write_quicklook_to_mp4

from punchpipe.control.db import File, Flow
from punchpipe.control.util import get_database_session, load_pipeline_configuration


@task
def visualize_query_ready_files(session, pipeline_config: dict, reference_time: datetime):
    logger = get_run_logger()

    all_ready_files = []
    all_product_codes = []
    levels = ["0", "1", "2", "3", "Q", "L"]
    for level in levels:
        product_codes = construct_all_product_codes(level=level)
        for product_code in product_codes:
            product_ready_files = (session.query(File)
                                    .filter(File.state.in_(["created", "progressed"]))
                                    .filter(File.date_obs >= (reference_time - timedelta(hours=24)))
                                    .filter(File.date_obs <= reference_time)
                                    .filter(File.level == level)
                                    .filter(File.file_type == product_code[0:2])
                                    .filter(File.observatory == product_code[2]).all())
            all_ready_files.append(list(product_ready_files))
            all_product_codes.append(f"L{level}_{product_code}")

    logger.info(f"{len(all_ready_files)} files will be used for visualization.")
    return all_ready_files, all_product_codes


@task
def visualize_flow_info(input_files: list[File],
                        product_code: str,
                        pipeline_config: dict,
                        reference_time: datetime,
                        session=None
                        ):
    flow_type = "movie"
    state = "planned"
    creation_time = datetime.now()
    priority = pipeline_config["flows"][flow_type]["priority"]["initial"]
    call_data = json.dumps(
        {
            "file_list": [
                os.path.join(input_file.directory(pipeline_config["root"]), input_file.filename())
                for input_file in input_files
            ],
            "product_code": product_code,
            "output_movie_dir": os.path.join(pipeline_config["root"], "movies")
        }
    )
    return Flow(
        flow_type=flow_type,
        state=state,
        flow_level="M",
        creation_time=creation_time,
        priority=priority,
        call_data=call_data,
    )


@flow
def movie_scheduler_flow(pipeline_config_path=None, session=None, reference_time: datetime | None = None):
    if session is None:
        session = get_database_session()

    reference_time = reference_time or datetime.now(UTC)

    pipeline_config = load_pipeline_configuration(pipeline_config_path)

    file_lists, product_codes = visualize_query_ready_files(session, pipeline_config, reference_time)

    for file_list, product_code in zip(file_lists, product_codes):
        flow = visualize_flow_info(file_list, product_code, pipeline_config, reference_time, session)
        session.add(flow)

    session.commit()


def quicklook_generator(file_list: list, product_code: str, output_movie_dir: str) -> None:
    tempdir = tempfile.TemporaryDirectory()

    annotation = "{OBSRVTRY} - {TYPECODE}{OBSCODE} - {DATE-OBS} - polarizer: {POLAR} deg"
    written_list = []
    if file_list:
        for i, cube_file in enumerate(file_list):
            cube = load_ndcube_from_fits(cube_file)

            if i == 0:
                obs_start = cube.meta["DATE-OBS"].value
            if i == len(file_list)-1:
                obs_end = cube.meta["DATE-OBS"].value

            img_file = os.path.join(tempdir.name, os.path.splitext(os.path.basename(cube_file))[0] + '.jp2')

            written_list.append(img_file)

            write_ndcube_to_quicklook(cube, filename = img_file, annotation = annotation)


        out_filename = os.path.join(output_movie_dir, f"{product_code}_{obs_start}-{obs_end}.mp4")
        os.makedirs(os.path.dirname(out_filename), exist_ok=True)
        write_quicklook_to_mp4(files = written_list, filename = out_filename)

        tempdir.cleanup()


@flow
def movie_process_flow(flow_id: int, pipeline_config_path=None, session=None):
    if session is None:
        session = get_database_session()
    logger = get_run_logger()

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
    try:
        quicklook_generator(**flow_call_data)
    except Exception as e:
        flow_db_entry.state = "failed"
        flow_db_entry.end_time = datetime.now(UTC)
        session.commit()
        raise e
    else:
        flow_db_entry.state = "completed"
        flow_db_entry.end_time = datetime.now(UTC)
        # Note: the file_db_entry gets updated above in the writing step because it could be created or blank
        session.commit()
