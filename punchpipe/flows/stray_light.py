import json
import typing as t
from datetime import datetime, timedelta

from prefect import flow, get_run_logger
from punchbowl.level1.stray_light import estimate_stray_light

from punchpipe import __version__
from punchpipe.control.db import File, Flow
from punchpipe.control.processor import generic_process_flow_logic
from punchpipe.control.scheduler import generic_scheduler_flow_logic
from punchpipe.control.util import get_database_session, load_pipeline_configuration
from punchpipe.flows.util import file_name_to_full_path


def construct_stray_light_query_ready_files(session,
                                            pipeline_config: dict,
                                            reference_time: datetime,
                                            reference_file: File,
                                            spacecraft: str,
                                            file_type: str):
    logger = get_run_logger()

    min_files_per_half = pipeline_config['flows']['construct_stray_light']['min_files_per_half']
    max_files_per_half = pipeline_config['flows']['construct_stray_light']['max_files_per_half']
    max_hours_per_half = pipeline_config['flows']['construct_stray_light']['max_hours_per_half']
    L0_impossible_after_days = pipeline_config['flows']['construct_stray_light']['new_L0_impossible_after_days']
    more_L0_impossible = datetime.now() - reference_time > timedelta(days=L0_impossible_after_days)

    t_start = reference_time - timedelta(hours=max_hours_per_half)
    t_end = reference_time + timedelta(hours=max_hours_per_half)
    file_type_mapping = {"SR": "XR", "SM": "XM", "SZ": "XZ", "SP": "XP"}
    target_file_type = file_type_mapping[file_type]
    L0_type_mapping = {"SR": "CR", "SM": "PM", "SZ": "PZ", "SP": "PP"}
    L0_target_file_type = L0_type_mapping[file_type]

    first_half_inputs = (session.query(File)
                       .filter(File.state.in_(["created", "progressed"]))
                       .filter(File.date_obs >= t_start)
                       .filter(File.date_obs <= reference_time)
                       .filter(File.level == "1")
                       .filter(File.file_type == target_file_type)
                       .filter(File.observatory == spacecraft)
                       .order_by(File.date_obs.desc())
                       .limit(max_files_per_half).all())

    second_half_inputs = (session.query(File)
                       .filter(File.state.in_(["created", "progressed"]))
                       .filter(File.date_obs >= reference_time)
                       .filter(File.date_obs <= t_end)
                       .filter(File.level == "1")
                       .filter(File.file_type == target_file_type)
                       .filter(File.observatory == spacecraft)
                       .order_by(File.date_obs.asc())
                       .limit(max_files_per_half).all())

    first_half_L0s = (session.query(File)
                       .filter(File.state.in_(["created", "progressed"]))
                       .filter(File.date_obs >= t_start)
                       .filter(File.date_obs <= reference_time)
                       .filter(File.level == "0")
                       .filter(File.file_type == L0_target_file_type)
                       .filter(File.observatory == spacecraft)
                       .order_by(File.date_obs.desc())
                       .limit(max_files_per_half).all())

    second_half_L0s = (session.query(File)
                       .filter(File.state.in_(["created", "progressed"]))
                       .filter(File.date_obs >= reference_time)
                       .filter(File.date_obs <= t_end)
                       .filter(File.level == "0")
                       .filter(File.file_type == L0_target_file_type)
                       .filter(File.observatory == spacecraft)
                       .order_by(File.date_obs.asc())
                       .limit(max_files_per_half).all())

    all_inputs_ready =  len(first_half_inputs) >= 0.95 * len(first_half_L0s) and len(second_half_inputs) >= 0.95 * len(second_half_L0s)
    enough_L1s = len(first_half_inputs) > min_files_per_half and len(second_half_inputs) > min_files_per_half
    max_L1s = len(first_half_inputs) == max_files_per_half and len(second_half_inputs) == max_files_per_half

    produce = False
    if more_L0_impossible:
        if len(first_half_L0s) < min_files_per_half or len(second_half_L0s) < min_files_per_half:
            reference_file.state = "impossible"
            # Record who deemed this to be impossible
            reference_file.file_version = pipeline_config["file_version"]
            reference_file.software_version = __version__
            reference_file.date_created = datetime.now()
        elif all_inputs_ready and enough_L1s:
            n = min(len(first_half_inputs), len(second_half_inputs))
            first_half_inputs = first_half_inputs[:n]
            second_half_inputs = second_half_inputs[:n]
            produce = True
    elif all_inputs_ready and max_L1s:
        produce = True

    if produce:
        all_ready_files = first_half_inputs + second_half_inputs

        logger.info(f"{len(all_ready_files)} Level 1 {target_file_type}{spacecraft} files will be used for stray light "
                     "estimation.")
        return [f.file_id for f in all_ready_files]
    return []


def construct_stray_light_flow_info(level1_files: list[File],
                                    level1_stray_light_file: File,
                                    pipeline_config: dict,
                                    reference_time: datetime,
                                    file_type: str,
                                    spacecraft: str,
                                    session=None):
    flow_type = "construct_stray_light"
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


def construct_stray_light_file_info(level1_files: t.List[File],
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
def construct_stray_light_scheduler_flow(pipeline_config_path=None, session=None, reference_time: datetime | None = None):
    session = get_database_session()
    pipeline_config = load_pipeline_configuration(pipeline_config_path)
    logger = get_run_logger()

    max_flows = 2 * pipeline_config['flows']['construct_stray_light'].get('concurrency_limit', 9e9)
    existing_flows = (session.query(Flow)
                       .where(Flow.flow_type == 'construct_stray_light')
                       .where(Flow.state.in_(["planned", "launched", "running"])).count())

    flows_to_schedule = max_flows - existing_flows
    if flows_to_schedule <= 0:
        logger.info("Our maximum flow count has been reached; halting")
    else:
        logger.info(f"Will schedule up to {flows_to_schedule} flows")

    existing_models = (session.query(File)
                       .filter(File.state.in_(["created", "planned", "creating", "impossible", "waiting"]))
                       .filter(File.level == "1")
                       .filter(File.file_type.in_(['SR', 'SZ', 'SP', 'SM']))
                       .all())
    logger.info(f"There are {len(existing_models)} model records in the DB")

    oldest_possible_input_file = (session.query(File)
                          .filter(File.state == "created")
                          .filter(File.level == "1")
                          .filter(File.file_type.in_(['XR', 'XZ', 'XP', 'XM']))
                          .order_by(File.date_obs.asc())
                          .first())
    if oldest_possible_input_file is None:
        logger.info("No possible input files in DB")
        return

    existing_models = {(model.file_type, model.observatory, model.date_obs): model for model in existing_models}
    t0 = datetime.strptime(pipeline_config['flows']['construct_stray_light']['t0'], "%Y-%m-%d %H:%M:%S")
    increment = timedelta(hours=pipeline_config['flows']['construct_stray_light']['model_spacing_hours'])
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
        for model_type in ['SR', 'SM', 'SZ', 'SP']:
            for observatory in ['1', '2', '3', '4']:
                key = (model_type, observatory, t)
                model = existing_models.get(key, None)
                if model is None:
                    new_model = File(state='waiting',
                                     level='1',
                                     file_type=model_type,
                                     observatory=observatory,
                                     date_obs=t,
                                     date_created=datetime.now(),
                                     file_version=pipeline_config["file_version"],
                                     polarization='C' if model_type[1] == 'R' else model_type[1],
                                     software_version=__version__)
                    session.add(new_model)
                    models_to_try_creating.append(new_model)
                elif model.state == 'waiting':
                    models_to_try_creating.append(model)

    logger.info(f"There are {len(models_to_try_creating)} un-created models")

    to_schedule = []
    for model in models_to_try_creating:
        ready_files = construct_stray_light_query_ready_files(
            session, pipeline_config, model.date_obs, model, model.observatory, model.file_type)
        if ready_files:
            to_schedule.append(ready_files)
            # Clear the placeholder model entry
            session.delete(model)
            logger.info(f"Will schedule {model.file_type} at {model.date_obs}")
            if len(to_schedule) == flows_to_schedule:
                break

    if len(to_schedule):
        args_dictionary = {"file_type": model.file_type, "spacecraft": model.observatory}

        generic_scheduler_flow_logic(
            lambda *args, **kwargs: to_schedule,
            construct_stray_light_file_info,
            construct_stray_light_flow_info,
            pipeline_config,
            update_input_file_state=False,
            session=session,
            args_dictionary=args_dictionary
        )

        logger.info(f"Scheduled {len(to_schedule)} models")


def construct_stray_light_call_data_processor(call_data: dict, pipeline_config, session) -> dict:
    # Prepend the directory path to each input file
    call_data['filepaths'] = file_name_to_full_path(call_data['filepaths'], pipeline_config['root'])
    call_data['num_workers'] = 32
    return call_data

@flow
def construct_stray_light_process_flow(flow_id: int, pipeline_config_path=None, session=None):
    generic_process_flow_logic(flow_id, estimate_stray_light, pipeline_config_path, session=session,
                               call_data_processor=construct_stray_light_call_data_processor)
