from punchpipe.controlsegment.db import Flow, MySQLCredentials
from punchpipe.controlsegment.scheduler import schedule_flow, schedule_file


from sqlalchemy import and_, create_engine
from sqlalchemy.orm import Session
from prefect import flow, task
from punchbowl.level1.flow import level1_core_flow
import time


@task
def level1_query_ready_flows(session):
    return [f.flow_id for f in session.query(Flow).where(and_(Flow.state == "finished", Flow.level == 0)).all()]


@task
def level1_construct_flow_schedule_info(session, flow_id):
    pass

@task
def level1_construct_file_info(session, flow_id):
    pass


@flow
def level1_scheduler_flow():
    credentials = MySQLCredentials.load("mysql-cred")
    engine = create_engine(
        f'mysql+pymysql://{credentials.user}:{credentials.password.get_secret_value()}@localhost/punchpipe')
    session = Session(engine)

    ready_flow_ids = level1_query_ready_flows(session)
    for flow_id in ready_flow_ids:
        database_schedule_info = level1_construct_flow_schedule_info(session, flow_id)
        schedule_flow(session, database_schedule_info)

        database_file_info = level1_construct_file_info(session, flow_id)
        schedule_file(session, database_file_info)


@flow
def level1_process_flow(input_filename, output_directory):
    level1_core_flow(input_filename, output_directory)

