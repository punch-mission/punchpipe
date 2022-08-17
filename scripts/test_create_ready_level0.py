from prefect import flow, task
from datetime import datetime
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from punchpipe.controlsegment.db import Flow, File, MySQLCredentials


@task
def construct_fake_entries():
    fake_file = File(level=0,
                     file_type="XX",
                     observatory="X",
                     file_version=0,
                     software_version=0,
                     date_created=datetime.now(),
                     date_beg=datetime.now(),
                     date_obs=datetime.now(),
                     date_end=datetime.now(),
                     polarization="XX",
                     state="created",
                     processing_flow=1)

    fake_flow = Flow(flow_type="Level 0",
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


@flow
def create_fake_level0():
    fake_flow, fake_file = construct_fake_entries()
    insert_into_table(fake_flow, fake_file)


if __name__ == "__main__":
    create_fake_level0()
