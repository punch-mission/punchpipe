from datetime import datetime
import json

from sqlalchemy import and_, create_engine
from sqlalchemy.orm import Session
from prefect import flow, task
from prefect.context import get_run_context
from punchbowl.level2.flow import level2_core_flow

from punchpipe.controlsegment.db import Flow, MySQLCredentials, File, FileRelationship
from punchpipe.controlsegment.scheduler import update_file_state


@task
def level2_query_ready_files(session):
    return [f.file_id for f in session.query(File).where(and_(File.state == "created", File.level == 1)).all()]


@task
def level2_construct_flow_info(level1_file: File, level2_file: File):
    flow_type = "level2_process_flow"
    state = "planned"
    creation_time = datetime.now()
    priority = 1
    call_data = json.dumps({"input_filename": level1_file.filename(), "output_filename": level2_file.filename()})
    return Flow(flow_type=flow_type,
                state=state,
                creation_time=creation_time,
                priority=priority,
                call_data=call_data)


@task
def level1_construct_file_info(level1_file: File):
    return File(level=2,
                file_type=level1_file.file_type,
                observatory=level1_file.observatory,
                file_version=0,  # TODO: decide how to implement this
                software_version=0,  # TODO: decide how to implement this
                date_obs=level1_file.date_obs,
                polarization=level1_file.polarization,
                state="planned")


@flow
def level2_scheduler_flow():
    # get database connection
    credentials = MySQLCredentials.load("mysql-cred")
    engine = create_engine(
        f'mysql+pymysql://{credentials.user}:{credentials.password.get_secret_value()}@localhost/punchpipe')
    session = Session(engine)

    # find all files that are ready to run
    ready_file_ids = level2_query_ready_files(session)
    for file_id in ready_file_ids:
        # mark the file as progressed so that there aren't duplicate processing flows
        update_file_state(session, file_id, "progressed")

        # get the level0 file's information
        level1_file = session.query(File).where(File.file_id == file_id).one()

        # prepare the new level 1 flow and file
        level2_file = level1_construct_file_info(level1_file)
        database_flow_info = level2_construct_flow_info(level1_file, level2_file)
        session.add(level2_file)
        session.add(database_flow_info)
        session.commit()

        # set the processing flow now that we know the flow_id after committing the flow info
        level2_file.processing_flow = database_flow_info.flow_id
        session.commit()

        # create a file relationship between the level 0 and level 1
        session.add(FileRelationship(parent=level1_file.file_id, child=level2_file.file_id))
        session.commit()


@flow
def level2_process_flow(flow_id: int):
    credentials = MySQLCredentials.load("mysql-cred")
    engine = create_engine(
        f'mysql+pymysql://{credentials.user}:{credentials.password.get_secret_value()}@localhost/punchpipe')
    session = Session(engine)

    # fetch the appropriate flow db entry
    flow_db_entry = session.query(Flow).where(Flow.flow_id == flow_id).one()

    # update the processing flow name with the flow run name from Prefect
    flow_run_context = get_run_context()
    flow_db_entry.flow_run = flow_run_context.flow_run.name
    flow_db_entry.state = "running"
    flow_db_entry.start_time = datetime.now()
    session.commit()

    # update the file database entry as being created
    file_db_entry = session.query(File).where(File.processing_flow == flow_db_entry.flow_id).one()
    file_db_entry.state = "creating"
    session.commit()

    # load the call data and launch the core flow
    flow_call_data = json.loads(flow_db_entry.call_data)
    try:
        level2_core_flow(flow_call_data['input_filename'], flow_call_data['output_filename'])
    except Exception as e:
        flow_db_entry.state = "failed"
        file_db_entry.state = "failed"
        flow_db_entry.end_time = datetime.now()
        session.commit()
        raise e
    else:
        flow_db_entry.state = "completed"
        file_db_entry.state = "created"
        flow_db_entry.end_time = datetime.now()
        session.commit()
