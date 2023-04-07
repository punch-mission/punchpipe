from datetime import datetime
import json
import os

import numpy as np
import astropy.wcs
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


def generate_fake_level0_data():
    data = np.random.rand(2048, 2048)
    wcs = astropy.wcs.WCS(naxis=3)
    wcs.wcs.ctype = "WAVE", "HPLT-TAN", "HPLN-TAN"
    wcs.wcs.cunit = "Angstrom", "deg", "deg"
    wcs.wcs.cdelt = 0.2, 0.5, 0.4
    wcs.wcs.crpix = 0, 2, 2
    wcs.wcs.crval = 10, 0.5, 1
    wcs.wcs.cname = "wavelength", "HPC lat", "HPC lon"

    meta = NormalizedMetadata({"OBSRVTRY": "Y", "LEVEL": 0, "TYPECODE": "XX", "DATE-OBS": str(datetime.now())},
        required_fields=PUNCH_REQUIRED_META_FIELDS)
    data = PUNCHData(data=data, wcs=wcs, meta=meta, uncertainty=np.zeros_like(data))
    return data


@flow
def create_fake_level0():
    fake_flow, fake_file = construct_fake_entries()
    fake_data = generate_fake_level0_data()
    output_directory = fake_file.directory("/home/marcus.hughes/running_test/")
    if not os.path.isdir(output_directory):
        os.makedirs(output_directory)
    output_path = os.path.join(output_directory, fake_file.filename())
    fake_data.write(output_path)
    insert_into_table(fake_flow, fake_file)


if __name__ == "__main__":
    create_fake_level0()
