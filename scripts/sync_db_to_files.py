"""This is for if you're keeping a mirror of the L0 files on punch190 (to process to L1 with newer code). This script
will add the files in the given directories to the database, skipping files that are already in the database."""
import os
import sys
import multiprocessing
from glob import glob
from datetime import datetime

from astropy.io import fits
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map

from punchpipe.control.db import File
from punchpipe.control.util import get_database_session


def read_new_file_metadata(file, path):
    # Get the correct number of microsseconds from the FITS header
    if file.file_type[0] in ['M', 'L']:
        return None, None

    with fits.open(path, disable_image_compression=True) as hdul:
        if len(hdul) > 1 and 'DATE-OBS' in hdul[1].header:
            p = hdul[1].header['DATE-OBS'].split('.')
            if len(p) == 2:
                ms = p[1]
                ms = ms + '0' * (6 - len(ms))
                new_dateobs = file.date_obs.replace(microsecond=int(ms))
        if len(hdul) > 1 and 'DATE' in hdul[1].header:
            p = hdul[1].header['DATE'].split('.')
            if len(p) == 2:
                ms = p[1]
                ms = ms + '0' * (6 - len(ms))
                new_date_created = file.date_obs.replace(microsecond=int(ms))
    return new_dateobs, new_date_created

if __name__ == "__main__":
    multiprocessing.set_start_method("forkserver")
    root_dirs = sys.argv[1:]

    session = get_database_session()

    files = set()
    for root_dir in root_dirs:
        files.update(glob(f"{root_dir}/**/*.fits", recursive=True))
        files.update(glob(f"{root_dir}/**/MS*/**/*.bin", recursive=True))
        files.update(glob(f"{root_dir}/**/L*/**/*.npz", recursive=True))

    print(f"Found {len(files)} files on disk")

    existing_files = session.query(File).all()

    existing_files = {f.filename() for f in existing_files}

    print(f"Loaded {len(existing_files)} existing files from the DB")

    n_added = 0
    n_existing = 0
    new_files = []
    new_file_paths = []
    print("Identifying new files...")
    for path in tqdm(files):
        base_path = os.path.basename(path)
        level = base_path.split("_")[1][1]
        code = base_path.split("_")[2][:2]
        obs = base_path.split("_")[2][-1]
        version = base_path.split("_")[-1].split(".")[0][1:]
        date = datetime.strptime(base_path.split("_")[3], "%Y%m%d%H%M%S")

        pol = 'C'
        if code[0] in ['G', 'S', 'R', 'P', 'L']:
            pol = code[1]
            if pol == 'R':
                pol = 'C'
        if code[0] == 'X':
            pol = 'X'

        file = File(
            level=level,
            file_type=code,
            observatory=obs,
            file_version=version,
            software_version='imported to db',
            date_obs=date,
            polarization=pol,
            state='created',
        )

        if file.filename() not in existing_files:
            new_files.append(file)
            new_file_paths.append(path)
            n_added += 1
        else:
            n_existing += 1


    print(f"Found {n_added} new files, skipping {n_existing} existing files")

    print("Reading metadata...")
    for file, (date_obs, date_created) in zip(
            new_files, process_map(read_new_file_metadata, new_files, new_file_paths, chunksize=10, max_workers=10)):
        if date_obs is not None:
            file.date_obs = date_obs
            file.date_created = date_created

    print("Adding to DB...")
    session.bulk_save_objects(new_files)
    session.commit()
