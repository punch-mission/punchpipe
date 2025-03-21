"""Run the entire pipeline backward."""
import os
import glob
import json
from datetime import UTC, datetime, timedelta

import numpy as np
from dateutil.parser import parse as parse_datetime_str
from prefect import flow, get_run_logger
from prefect.context import get_run_context
from simpunch.level0 import generate_l0_cr, generate_l0_pmzp
from simpunch.level1 import generate_l1_cr, generate_l1_pmzp
from simpunch.level2 import generate_l2_ctm, generate_l2_ptm
from simpunch.level3 import generate_l3_ctm, generate_l3_ptm

from punchpipe.control.db import Flow
from punchpipe.control.util import get_database_session, load_pipeline_configuration


def simulate_flow(file_tb: str,
                  file_pb: str,
                  out_dir: str,
                  time_obs: datetime,
                  backward_psf_model_path: str,
                  wfi_quartic_backward_model_path: str,
                  nfi_quartic_backward_model_path: str,
                  transient_probability: float = 0.03,
                  shift_pointing: bool = False) -> bool:
    """Generate all the products in the reverse pipeline."""
    i = int(os.path.basename(file_tb).split("_")[4][4:])  # get the index from the filename to determine the rotation
    rotation_indices = np.array([0, 0, 1, 1, 2, 2, 3, 3])
    rotation_stage = rotation_indices[i % 8]
    l3_ptm = generate_l3_ptm(file_tb, file_pb, out_dir, time_obs, timedelta(minutes=4), rotation_stage)
    l3_ctm = generate_l3_ctm(file_tb, out_dir, time_obs, timedelta(minutes=4), rotation_stage)
    l2_ptm = generate_l2_ptm(l3_ptm, out_dir)
    l2_ctm = generate_l2_ctm(l3_ctm, out_dir)

    l1_polarized = []
    l1_clear = []
    for spacecraft in ["1", "2", "3", "4"]:
        l1_polarized.extend(generate_l1_pmzp(l2_ptm, out_dir, rotation_stage, spacecraft))
        l1_clear.append(generate_l1_cr(l2_ctm, out_dir, rotation_stage, spacecraft))

    for filename in l1_polarized:
        generate_l0_pmzp(filename, out_dir, backward_psf_model_path,
                                               wfi_quartic_backward_model_path, nfi_quartic_backward_model_path,
                                               transient_probability, shift_pointing)

    for filename in l1_clear:
        generate_l0_cr(filename, out_dir, backward_psf_model_path,
                                               wfi_quartic_backward_model_path, nfi_quartic_backward_model_path,
                                               transient_probability, shift_pointing)
    return True


@flow
def simpunch_scheduler_flow(pipeline_config_path=None, session=None, reference_time: datetime | str | None = None):
    pipeline_config = load_pipeline_configuration(pipeline_config_path)
    flow_type = "simpunch"
    state = "planned"
    creation_time = datetime.now()
    priority = pipeline_config["flows"][flow_type]["priority"]["initial"]

    call_data = json.dumps(
        {
            "date_obs": reference_time or str(datetime.now(UTC)),
            "simulation_start": pipeline_config["flows"][flow_type]["options"].get("simulation_start", ""),
            "simulation_cadence_minutes": pipeline_config["flows"][flow_type]["options"].get("simulation_cadence_minutes", 4.0),
            "gamera_files_dir": pipeline_config["flows"][flow_type]["options"].get("gamera_files_dir", ""),
            "out_dir": pipeline_config["flows"][flow_type]["options"].get("out_dir", ""),
            "backward_psf_model_path": pipeline_config["flows"][flow_type]["options"].get("backward_psf_model_path", ""),
            "wfi_quartic_backward_model_path": pipeline_config["flows"][flow_type]["options"].get("wfi_quartic_backward_model_path", ""),
            "nfi_quartic_backward_model_path": pipeline_config["flows"][flow_type]["options"].get("nfi_quartic_backward_model_path", ""),
            "transient_probability": pipeline_config["flows"][flow_type]["options"].get("transient_probability", 0),
            "shift_pointing": pipeline_config["flows"][flow_type]["options"].get("shift_pointing", False)
        }
    )
    new_flow = Flow(
        flow_type=flow_type,
        flow_level="S",
        state=state,
        creation_time=creation_time,
        priority=priority,
        call_data=call_data,
    )

    if session is None:
        session = get_database_session()

    session.add(new_flow)
    session.commit()

@flow
def simpunch_core_flow(
        date_obs: datetime | str,
        simulation_start: datetime | str,
        simulation_cadence_minutes: float,
        gamera_files_dir: str,
        out_dir: str,
        backward_psf_model_path: str,
        wfi_quartic_backward_model_path: str,
        nfi_quartic_backward_model_path: str,
        transient_probability: float = 0.03,
        shift_pointing: bool = False):

    logger = get_run_logger()

    if isinstance(date_obs, str):
        date_obs = parse_datetime_str(date_obs).replace(tzinfo=UTC)
    if isinstance(simulation_start, str):
        simulation_start = parse_datetime_str(simulation_start).replace(tzinfo=UTC)

    logger.info(f"Running for {date_obs}")

    tb_files = sorted(glob.glob(gamera_files_dir + "/*_TB.fits"))
    pb_files = sorted(glob.glob(gamera_files_dir + "/*_PB.fits"))
    simulation_frame_count = len(tb_files)

    minutes_after_start = (date_obs - simulation_start).total_seconds() / 60
    raw_index = minutes_after_start / simulation_cadence_minutes
    index = int(raw_index % simulation_frame_count)
    logger.info(f"Running on index {index}")

    file_tb = tb_files[index]
    file_pb = pb_files[index]
    logger.info(f"file_tb = {file_tb}")
    logger.info(f"file_pb = {file_pb}")

    simulate_flow(file_tb, file_pb, out_dir, date_obs, backward_psf_model_path,wfi_quartic_backward_model_path,
                  nfi_quartic_backward_model_path, transient_probability, shift_pointing)


@flow
def simpunch_process_flow(flow_id: int, pipeline_config_path=None, session=None):
    logger = get_run_logger()

    if session is None:
        session = get_database_session()

    # fetch the appropriate flow db entry
    flow_db_entry = session.query(Flow).where(Flow.flow_id == flow_id).one()
    logger.info(f"Running on flow db entry with id={flow_db_entry.flow_id}.")

    # update the processing flow name with the flow run name from Prefect
    flow_run_context = get_run_context()
    flow_db_entry.flow_run_name = flow_run_context.flow_run.name
    flow_db_entry.flow_run_id = flow_run_context.flow_run.id
    flow_db_entry.state = "running"
    flow_db_entry.start_time = datetime.now()
    session.commit()

    # load the call data and launch the core flow
    flow_call_data = json.loads(flow_db_entry.call_data)
    logger.info(f"Running with {flow_call_data}")
    try:
        simpunch_core_flow(**flow_call_data)
    except Exception as e:
        flow_db_entry.state = "failed"
        flow_db_entry.end_time = datetime.now()
        session.commit()
        raise e
    else:
        flow_db_entry.state = "completed"
        flow_db_entry.end_time = datetime.now()
        # Note: the file_db_entry gets updated above in the writing step because it could be created or blank
        session.commit()
