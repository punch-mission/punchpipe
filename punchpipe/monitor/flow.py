from typing import List

from prefect import flow, task
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from punchpipe.controlsegment.db import Flow, MySQLCredentials


@task
def count_active_flows(session) -> int:
    return len(session.query(Flow).where(Flow.state == "running").all())


@task
def determine_zombie_flows(session) -> List[Flow]:
    return []


@task
def format_pdf(active_flow_count, zombie_flow_list):
    pass


@flow
def monitoring_flow():
    credentials = MySQLCredentials.load("mysql-cred")
    engine = create_engine(
        f"mysql+pymysql://{credentials.user}:{credentials.password.get_secret_value()}@localhost/punchpipe"
    )
    session = Session(engine)

    num_active_flows = count_active_flows(session)
    zombie_flow_list = determine_zombie_flows(session)

    format_pdf(num_active_flows, zombie_flow_list)
