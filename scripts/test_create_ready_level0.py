from datetime import datetime
import json
import os

import numpy as np
from astropy.nddata import StdDevUncertainty
from astropy.wcs import WCS
from prefect import flow, task
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from punchbowl.data import PUNCHData, NormalizedMetadata, PUNCH_REQUIRED_META_FIELDS

from punchpipe.controlsegment.db import Flow, File, MySQLCredentials


@task
def construct_fake_entries():
    fake_file = File(level=0,
                     file_type="XX",
                     observatory="Y",
                     file_version="0",
                     software_version="0",
                     date_created=datetime.now(),
                     date_beg=datetime.now(),
                     date_obs=datetime.now(),
                     date_end=datetime.now(),
                     polarization="XX",
                     state="created",
                     processing_flow=1)

    fake_flow = Flow(flow_type="Level 0",
                     flow_level=0,
                     state="completed",
                     creation_time=datetime.now(),
                     start_time=datetime.now(),
                     end_time=datetime.now(),
                     priority=1,
                     call_data=json.dumps({"input_filename": "input_test", "output_filename": "output_test"}))

    return fake_flow, fake_file


@task
def insert_into_table(fake_flow, fake_file):
    credentials = MySQLCredentials.load("mysql-cred")
    engine = create_engine(
        f'mysql+pymysql://{credentials.user}:{credentials.password.get_secret_value()}@localhost/punchpipe')
    session = Session(engine)

    session.add(fake_flow)
    session.commit()

    session.add(fake_file)
    session.commit()

    fake_file.processing_flow = fake_flow.flow_id
    session.commit()


def generate_fake_level0_data(date_obs):
    shape = (2048, 2048)
    data = np.random.random(shape)
    uncertainty = StdDevUncertainty(np.sqrt(np.abs(data)))
    wcs = WCS(naxis=2)
    wcs.wcs.ctype = "HPLN-ARC", "HPLT-ARC"
    wcs.wcs.cunit = "deg", "deg"
    wcs.wcs.cdelt = 0.1, 0.1
    wcs.wcs.crpix = 0, 0
    wcs.wcs.crval = 1, 1
    wcs.wcs.cname = "HPC lon", "HPC lat"

    meta = NormalizedMetadata({"LEVEL": str(0),
                               'OBSRVTRY': 'Y',
                               'TYPECODE': 'XX',
                               'DATE-OBS': str(date_obs)},
                              required_fields=PUNCH_REQUIRED_META_FIELDS)
    return PUNCHData(data=data, uncertainty=uncertainty, wcs=wcs, meta=meta)


@flow
def create_fake_level0():
    fake_flow, fake_file = construct_fake_entries()
    fake_data = generate_fake_level0_data(fake_file.date_obs)
    output_directory = fake_file.directory("/home/marcus.hughes/running_test/")
    if not os.path.isdir(output_directory):
        os.makedirs(output_directory)
    output_path = os.path.join(output_directory, fake_file.filename())
    fake_data.write(output_path)
    insert_into_table(fake_flow, fake_file)


if __name__ == "__main__":
    create_fake_level0()
