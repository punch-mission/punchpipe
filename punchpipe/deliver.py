import hashlib
import os
from zipfile import ZipFile
import time
from datetime import datetime, timedelta

import pandas as pd
from prefect import task, flow

from punchpipe.controlsegment.util import get_database_session, get_files_in_time_window


def hash_one_file(path):
    file_hash = hashlib.sha256()
    with open(path, 'rb') as f:
        fb = f.read()
        file_hash.update(fb)

    return file_hash.hexdigest()

@task
def build_noaa_manifest(file_paths):
    hashes = [hash_one_file(file_path) for file_path in file_paths]
    return pd.DataFrame({'file': file_paths, 'hash': hashes})


@task
def get_noaa_files_in_time_range(start_time, end_time, session=None):
    if session is None:
        session = get_database_session()

    paths = []
    for obs_code in ["1", "2", "3", "4"]:
        paths += get_files_in_time_window("Q", "CR", obs_code, start_time, end_time, session=session)
    return paths


@flow
def create_noaa_delivery(time_window=timedelta(hours=1)):
    end_time = datetime.now()
    start_time = end_time - time_window
    timestamp = end_time.strftime('%Y%m%d%H%M%S')

    file_paths = get_noaa_files_in_time_range(start_time, end_time)

    manifest = build_noaa_manifest(file_paths)
    manifest_path = f'manifest_{timestamp}.csv'
    manifest.to_csv(manifest_path)

    zip_path = f'noaa_{timestamp}.zip'
    with ZipFile(zip_path, 'w') as zip_object:
        for file_path in file_paths:
            zip_object.write(file_path)
        zip_object.write(manifest_path)

    return zip_path
