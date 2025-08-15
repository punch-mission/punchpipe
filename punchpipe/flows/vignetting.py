import json
import typing as t
from datetime import datetime, timedelta

from prefect import flow, get_run_logger
from punchbowl.level1.vignette import generate_vignetting_calibration_nfi

from punchpipe import __version__
from punchpipe.control.db import File, Flow
from punchpipe.control.processor import generic_process_flow_logic
from punchpipe.control.scheduler import generic_scheduler_flow_logic
from punchpipe.control.util import get_database_session, load_pipeline_configuration
from punchpipe.flows.util import file_name_to_full_path


def construct_vignette_query_ready_files(session,
                                          pipeline_config: dict,
                                          reference_time: datetime,
                                          spacecraft: str,
                                          file_type: str):
    logger = get_run_logger()

    be_lenient = ((datetime.now() - reference_time).total_seconds()
                  > pipeline_config['flows']['construct_vignette']['be_lenient_after_days'] * 24*60*60)

    n_files_per_half = pipeline_config['flows']['construct_vignette']['n_files_per_half']
    n_files_per_half_lenient = pipeline_config['flows']['construct_vignette']['lenient_n_files_per_half']

    if be_lenient:
        time_window = pipeline_config['flows']['construct_vignette']['lenient_max_hours_per_half']
        min_files = n_files_per_half_lenient
    else:
        time_window = pipeline_config['flows']['construct_vignette']['max_hours_per_half']
        min_files = n_files_per_half

    t_start = reference_time - timedelta(hours=time_window)
    t_end = reference_time + timedelta(hours=time_window)
    file_type_mapping = {"GR": "XR", "GM": "XM", "GZ": "XZ", "GP": "XP"}
    target_file_type = file_type_mapping[file_type]

    first_half_inputs = (session.query(File)
                       .filter(File.state.in_(["created", "progressed"]))
                       .filter(File.date_obs >= t_start)
                       .filter(File.date_obs <= reference_time)
                       .filter(File.level == "1")
                       .filter(File.file_type == target_file_type)
                       .filter(File.observatory == spacecraft)
                       .order_by(File.date_obs.desc())
                       .limit(n_files_per_half).all())
    if len(first_half_inputs) < min_files:
        return []

    second_half_inputs = (session.query(File)
                       .filter(File.state.in_(["created", "progressed"]))
                       .filter(File.date_obs >= reference_time)
                       .filter(File.date_obs <= t_end)
                       .filter(File.level == "1")
                       .filter(File.file_type == target_file_type)
                       .filter(File.observatory == spacecraft)
                       .order_by(File.date_obs.asc())
                       .limit(n_files_per_half).all())
    if len(second_half_inputs) < min_files:
        return []

    files_per_side = min(len(first_half_inputs), len(second_half_inputs))

    all_ready_files = first_half_inputs[:files_per_side] + second_half_inputs[:files_per_side]

    most_recent_date_created = min(f.date_created for f in all_ready_files)
    if datetime.now() - most_recent_date_created < timedelta(minutes=30):
        # This is a guard primarily for reprocessing---if any of these
        # files were recently written, the time range is probably being actively processed, so let's defer.
        logger.info(f"For {reference_time} {file_type}{spacecraft}, a file was written recently. Deferring creation.")
        return []

    logger.info(f"{len(all_ready_files)} Level 1 {target_file_type}{spacecraft} files will be used for stray light "
                 "estimation.")
    return [[f.file_id for f in all_ready_files]]


def construct_vignette_flow_info(level1_files: list[File],
                                    level1_vignette_file: File,
                                    pipeline_config: dict,
                                    reference_time: datetime,
                                    file_type: str,
                                    spacecraft: str,
                                    session=None):
    flow_type = "construct_vignette"
    state = "planned"
    creation_time = datetime.now()
    priority = pipeline_config["flows"][flow_type]["priority"]["initial"]
    call_data = json.dumps(
        {
            "filepaths": [level1_file.filename() for level1_file in level1_files],
            "reference_time": reference_time.strftime("%Y-%m-%d %H:%M:%S"),
        }
    )
    return Flow(
        flow_type=flow_type,
        state=state,
        flow_level="1",
        creation_time=creation_time,
        priority=priority,
        call_data=call_data,
    )


def construct_vignette_file_info(level1_files: t.List[File],
                                    pipeline_config: dict,
                                    reference_time: datetime,
                                    file_type: str,
                                    spacecraft: str) -> t.List[File]:
    date_obses = [f.date_obs for f in level1_files]
    return [File(
                level="1",
                file_type=file_type,
                observatory=spacecraft,
                file_version=pipeline_config["file_version"],
                software_version=__version__,
                date_obs=reference_time,
                date_beg=min(date_obses),
                date_end=max(date_obses),
                polarization=level1_files[0].polarization,
                state="planned",
            ),]

@flow
def construct_vignette_scheduler_flow(pipeline_config_path=None, session=None, reference_time: datetime | None = None):
    session = get_database_session()
    pipeline_config = load_pipeline_configuration(pipeline_config_path)
    logger = get_run_logger()

    max_flows = 2 * pipeline_config['flows']['construct_vignette'].get('concurrency_limit', 9e9)
    existing_flows = (session.query(Flow)
                       .where(Flow.flow_type == 'construct_vignette')
                       .where(Flow.state.in_(["planned", "launched", "running"])).count())
    flows_to_schedule = max_flows - existing_flows
    if flows_to_schedule <= 0:
        logger.info("Our maximum flow count has been reached; halting")
    else:
        logger.info(f"Will schedule up to {flows_to_schedule} flows")

    existing_models = (session.query(File)
                       .filter(File.state.in_(["created", "planned", "creating"]))
                       .filter(File.level == "1")
                       .filter(File.file_type.in_(['GR', 'GZ', 'GP', 'GM']))
                       .all())
    logger.info(f"There are {len(existing_models)} existing models")

    oldest_possible_input_file = (session.query(File)
                          .filter(File.state == "created")
                          .filter(File.level == "1")
                          .filter(File.file_type.in_(['XR', 'XZ', 'XP', 'XM']))
                          .order_by(File.date_obs.asc())
                          .first())
    if oldest_possible_input_file is None:
        logger.info("No possible input files in DB")
        return

    existing_models = set((model.file_type, model.observatory, model.date_obs) for model in existing_models)
    t0 = datetime.strptime(pipeline_config['flows']['construct_vignette']['t0'], "%Y-%m-%d %H:%M:%S")
    increment = timedelta(hours=pipeline_config['flows']['construct_vignette']['model_spacing_hours'])
    n = 0
    models_to_try_creating = []
    # I'm sure there's a better way to do this, but let's step forward by increments to the present, and then we'll go
    # backwards back to t0 scheduling, so that we prioritize the stray light models that QuickPUNCH uses
    while t0 + n * increment < datetime.now():
        n += 1
    for i in range(n, 0, -1):
        t = t0 + i * increment
        if t < oldest_possible_input_file.date_obs:
            # This can help speed this flow for the beginning of reprocessing
            break
        for model_type in ['GR', 'GM', 'GZ', 'GP']:
            for observatory in ['4']:
                key = (model_type, observatory, t)
                if key not in existing_models:
                    models_to_try_creating.append(key)

    logger.info(f"There are {len(models_to_try_creating)} un-created models")

    scheduled = []
    for model_type, observatory, t in models_to_try_creating:
        args_dictionary = {"file_type": model_type, "spacecraft": observatory}

        n_scheduled = generic_scheduler_flow_logic(
            construct_vignette_query_ready_files,
            construct_vignette_file_info,
            construct_vignette_flow_info,
            pipeline_config,
            update_input_file_state=False,
            reference_time=t,
            session=session,
            args_dictionary=args_dictionary
        )
        if n_scheduled > 0:
            scheduled.append((model_type + observatory, t))
        if len(scheduled) == flows_to_schedule:
            break

    logger.info(f"Scheduled {len(scheduled)} models")
    for type, t in scheduled:
        logger.info(f"Scheduled {type} at {t}")


def construct_vignette_call_data_processor(call_data: dict, pipeline_config, session) -> dict:
    # Prepend the directory path to each input file
    call_data['filepaths'] = file_name_to_full_path(call_data['filepaths'], pipeline_config['root'])
    call_data['num_workers'] = 32
    return call_data

@flow
def construct_vignette_process_flow(flow_id: int, pipeline_config_path=None, session=None):
    generic_process_flow_logic(flow_id, generate_vignetting_calibration_nfi,
                               pipeline_config_path, session=session, call_data_processor=construct_vignette_call_data_processor)
