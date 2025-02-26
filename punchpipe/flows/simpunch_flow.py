"""Run the entire pipeline backward."""
import glob
import os
import shutil
from datetime import datetime, timedelta

import numpy as np
from prefect import flow, task
from prefect.futures import wait
from prefect_ray import RayTaskRunner

from punchpipe.simpunch.level0 import generate_l0_all, generate_l0_pmzp, generate_l0_cr
from punchpipe.simpunch.level1 import generate_l1_all, generate_l1_pmzp, generate_l1_cr
from punchpipe.simpunch.level2 import generate_l2_all, generate_l2_ptm, generate_l2_ctm
from punchpipe.simpunch.level3 import generate_l3_all, generate_l3_all_fixed, generate_l3_ptm, generate_l3_ctm


def generate_flow(file_tb: str,
                  file_pb: str,
                  out_dir: str,
                  start_time: datetime,
                  backward_psf_model_path: str,
                  wfi_quartic_backward_model_path: str,
                  nfi_quartic_backward_model_path: str,
                  transient_probability: float = 0.03,
                  shift_pointing: bool = False) -> bool:
    """Generate all the products in the reverse pipeline."""
    i = int(file_tb.split("_")[6][4:])
    rotation_indices = np.array([0, 0, 1, 1, 2, 2, 3, 3])
    rotation_stage = rotation_indices[i % 8]
    print(i, rotation_stage)
    time_obs = start_time + timedelta(minutes=i*4)
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
    # if start_time is None:
    #     start_time = datetime.now() # noqa: DTZ005
    #
    # if generate_new:
    #     time_delta = timedelta(days=surrounding_cadence)
    #     files_tb = sorted(glob.glob(gamera_directory + "/synthetic_cme/*_TB.fits"))
    #     files_pb = sorted(glob.glob(gamera_directory + "/synthetic_cme/*_PB.fits"))
    #
    #     previous_month = [start_time + timedelta(days=td)
    #                       for td in np.linspace(1, -30, int(timedelta(days=30)/time_delta))]
    #     generate_l3_all_fixed(gamera_directory, output_directory, previous_month, files_pb[0], files_tb[0],
    #                           n_workers=n_workers)
    #
    #     next_month = [start_time + timedelta(days=td)
    #                       for td in np.linspace(1, 30, int(timedelta(days=30)/time_delta))]
    #     generate_l3_all_fixed(gamera_directory, output_directory, next_month, files_pb[-1], files_tb[-1],
    #                           n_workers=n_workers)
    #
    #     if generate_full_day:
    #         generate_l3_all(gamera_directory, output_directory, start_time,
    #                         num_repeats=num_repeats, n_workers=n_workers)
    #
    #     generate_l2_all(gamera_directory, output_directory, n_workers=n_workers)
    #     generate_l1_all(gamera_directory, output_directory, n_workers=n_workers)
    #     generate_l0_all(gamera_directory,
    #                     output_directory,
    #                     backward_psf_model_path,
    #                     wfi_quartic_backward_model_path,
    #                     nfi_quartic_backward_model_path,
    #                     shift_pointing=shift_pointing,
    #                     transient_probability=transient_probability,
    #                     n_workers=n_workers)
    #
    #     model_time = start_time - timedelta(days=35)
    #     model_time_str = model_time.strftime("%Y%m%d%H%M%S")
    #
    #     # duplicate the psf model to all required versions
    #     for type_code in ["RM", "RZ", "RP", "RC"]:
    #         for obs_code in ["1", "2", "3", "4"]:
    #             new_name = 	f"PUNCH_L1_{type_code}{obs_code}_{model_time_str}_v1.fits"
    #             shutil.copy(forward_psf_model_path, os.path.join(output_directory, f"synthetic_l0/{new_name}"))
    #
    #     # duplicate the quartic model
    #     type_code = "FQ"
    #     for obs_code in ["1", "2", "3"]:
    #         new_name = 	f"PUNCH_L1_{type_code}{obs_code}_{model_time_str}_v1.fits"
    #         shutil.copy(wfi_quartic_model_path, os.path.join(output_directory, f"synthetic_l0/{new_name}"))
    #     obs_code = "4"
    #     new_name = f"PUNCH_L1_{type_code}{obs_code}_{model_time_str}_v1.fits"
    #     shutil.copy(nfi_quartic_model_path, os.path.join(output_directory, f"synthetic_l0/{new_name}"))
    #
    # if update_database:
    #     from punchpipe import __version__
    #     from punchpipe.control.db import File
    #     from punchpipe.control.util import get_database_session
    #     db_session = get_database_session()
    #     for file_path in sorted(glob.glob(os.path.join(output_directory, "synthetic_l0/*v[0-9].fits")),
    #                             key=lambda s: os.path.basename(s)[13:27]):
    #         file_name = os.path.basename(file_path)
    #         level = file_name[7]
    #         file_type = file_name[9:11]
    #         observatory = file_name[11]
    #         year = file_name[13:17]
    #         month = file_name[17:19]
    #         day = file_name[19:21]
    #         hour = file_name[21:23]
    #         minute = file_name[23:25]
    #         second = file_name[25:27]
    #         dt = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))
    #         version = file_name.split(".fits")[0].split("_")[-1][1:]
    #
    #         output_dir = os.path.join(output_directory, level, file_type+observatory, year, month, day)
    #         os.makedirs(output_dir, exist_ok=True)
    #         shutil.copy(file_path, os.path.join(output_dir, file_name))
    #
    #         db_entry = File(
    #             level=level,
    #             file_type=file_type,
    #             observatory=observatory,
    #             file_version=version,
    #             software_version=__version__,
    #             date_obs=dt,
    #             polarization=file_type[1],
    #             state="created",
    #         )
    #         db_session.add(db_entry)
    #         db_session.commit()
    #
    # return True

@task
def wrapper(i):
    gamera_directory = "/d0/punchsoc/gamera_data/"
    files_tb = sorted(glob.glob(gamera_directory + "/synthetic_cme/*_TB.fits"))
    files_pb = sorted(glob.glob(gamera_directory + "/synthetic_cme/*_PB.fits"))

    generate_flow(
        file_tb = files_tb[i],
        file_pb = files_pb[i],
        out_dir = "/d0/punchsoc/gamera_data/outputs/",
        start_time=datetime(2020, 1, 1) + timedelta(days=i),
        backward_psf_model_path="/d0/punchsoc/gamera_data/inputs/synthetic_backward_psf.fits",
        wfi_quartic_backward_model_path="/d0/punchsoc/gamera_data/inputs/wfi_quartic_backward_coeffs.fits",
        nfi_quartic_backward_model_path="/d0/punchsoc/gamera_data/inputs/nfi_quartic_backward_coeffs.fits",
        transient_probability=0.0,
        shift_pointing=False,
    )

@flow(task_runner=RayTaskRunner())
def looper():
    futures = [wrapper.submit(i) for i in range(200)]
    wait(futures)

if __name__ == "__main__":
    looper()
