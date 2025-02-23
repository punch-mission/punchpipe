from datetime import datetime
from glob import glob
from punchpipe.control.db import File
from punchpipe.control.util import get_database_session
import shutil
import os
from punchpipe import __version__
from punchpipe.control.db import File
from punchpipe.control.util import get_database_session

paths = glob("/home/jmbhughes/data/simpunch/outputs_sleep/*L0_P*_v1.fits")
output_directory = "/home/jmbhughes/data/simpunch/"
forward_psf_model_path = "/home/jmbhughes/data/simpunch/inputs/synthetic_forward_psf.fits"
wfi_quartic_model_path = "/home/jmbhughes/data/simpunch/inputs/wfi_quartic_coeffs.fits"
nfi_quartic_model_path = "/home/jmbhughes/data/simpunch/inputs/nfi_quartic_coeffs.fits"
model_time = datetime(2000, 1, 1)

os.makedirs(os.path.join(output_directory, "synthetic_l0"), exist_ok=True)
session = get_database_session()

for path in paths:
    shutil.copy(path, os.path.join(output_directory, f"synthetic_l0/{os.path.basename(path)}"))

model_time_str = model_time.strftime("%Y%m%d%H%M%S")
for type_code in ["RM", "RZ", "RP", "RC"]:
    for obs_code in ["1", "2", "3", "4"]:
        new_name = 	f"PUNCH_L1_{type_code}{obs_code}_{model_time_str}_v1.fits"
        shutil.copy(forward_psf_model_path, os.path.join(output_directory, f"synthetic_l0/{new_name}"))

# duplicate the quartic model
type_code = "FQ"
for obs_code in ["1", "2", "3"]:
    new_name = 	f"PUNCH_L1_{type_code}{obs_code}_{model_time_str}_v1.fits"
    shutil.copy(wfi_quartic_model_path, os.path.join(output_directory, f"synthetic_l0/{new_name}"))
obs_code = "4"
new_name = f"PUNCH_L1_{type_code}{obs_code}_{model_time_str}_v1.fits"
shutil.copy(nfi_quartic_model_path, os.path.join(output_directory, f"synthetic_l0/{new_name}"))

db_session = get_database_session()
for file_path in sorted(glob(os.path.join(output_directory, "synthetic_l0/*v[0-9].fits")),
                        key=lambda s: os.path.basename(s)[13:27]):
    file_name = os.path.basename(file_path)
    level = file_name[7]
    file_type = file_name[9:11]
    observatory = file_name[11]
    year = file_name[13:17]
    month = file_name[17:19]
    day = file_name[19:21]
    hour = file_name[21:23]
    minute = file_name[23:25]
    second = file_name[25:27]
    dt = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))
    version = file_name.split(".fits")[0].split("_")[-1][1:]

    output_dir = os.path.join(output_directory, level, file_type+observatory, year, month, day)
    os.makedirs(output_dir, exist_ok=True)
    shutil.copy(file_path, os.path.join(output_dir, file_name))

    db_entry = File(
        level=level,
        file_type=file_type,
        observatory=observatory,
        file_version=version,
        software_version=__version__,
        date_obs=dt,
        polarization=file_type[1],
        state="created",
    )
    db_session.add(db_entry)
    db_session.commit()
