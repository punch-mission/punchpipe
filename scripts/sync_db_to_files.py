"""This is for if you're keeping a mirror of the L0 files on punch190 (to process to L1 with newer code). This script
will add the files in the given directories to the database, skipping files that are already in the database."""
import os
import sys
from glob import glob
from datetime import datetime

from astropy.io import fits

from punchpipe.control.db import File
from punchpipe.control.util import get_database_session

root_dirs = sys.argv[1:]

session = get_database_session()

files = set()
for root_dir in root_dirs:
    files.update(glob(f"{root_dir}/**/*.fits", recursive=True))
    files.update(glob(f"{root_dir}/**/MS*/**/*.bin", recursive=True))

print(f"Found {len(files)} files on disk")

print("TODO: This script probably doesn't set the 'polarization' column correctly for every polarized file code")


existing_files = session.query(File).all()

existing_files = {f.filename() for f in existing_files}

print(f"Loaded {len(existing_files)} existing files from the DB")

n_added = 0
n_existing = 0
for path in files:
    base_path = os.path.basename(path)
    level = base_path.split("_")[1][1]
    code = base_path.split("_")[2][:2]
    obs = base_path.split("_")[2][-1]
    version = base_path.split("_")[-1].split(".")[0][1:]
    date = datetime.strptime(base_path.split("_")[3], "%Y%m%d%H%M%S")

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

    if file.filename() not in existing_files:
        # Get the correct number of microsseconds from the FITS header
        if file.file_type != 'MS':
            with fits.open(path, disable_image_compression=True) as hdul:
                if len(hdul) > 1 and 'DATE-OBS' in hdul[1].header:
                    p = hdul[1].header['DATE-OBS'].split('.')
                    if len(p) == 2:
                        ms = p[1]
                        ms = ms + '0' * (6 - len(ms))
                        file.date_obs = file.date_obs.replace(microsecond=int(ms))
        session.add(file)
        n_added += 1
    else:
        n_existing += 1
session.commit()

print(f"Added {n_added} files, skipped {n_existing} files")
