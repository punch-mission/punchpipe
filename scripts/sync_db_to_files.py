import os
import sys
import shutil
from glob import glob
from datetime import datetime

from punchpipe.control.db import File
from punchpipe.control.util import get_database_session

root_dir = sys.argv[1]

session = get_database_session()

files = glob(f"{root_dir}/**/*.fits", recursive=True)
files += glob(f"{root_dir}/**/MS*/**/*.bin", recursive=True)

print("TODO: This script probably doesn't set the 'polarization' column correctly for every polarized file code")


for file in files:
    base_path = os.path.basename(file)
    level = base_path.split("_")[1][1]
    code = base_path.split("_")[2][:2]
    obs = base_path.split("_")[2][-1]
    date = datetime.strptime(base_path.split("_")[3], "%Y%m%d%H%M%S")
    version = base_path.split("_")[-1].split(".")[0][1:]

    file = File(
        level=level,
        file_type=code,
        observatory=obs,
        file_version=version,
        software_version='imported to db',
        date_obs=date,
        polarization=code[1] if code[0] == 'P' else 'C',
        state='created',
    )
    session.add(file)
session.commit()

print(f"Added {len(files)} files")