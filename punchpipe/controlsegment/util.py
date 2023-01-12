from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from prefect import task

from punchpipe.controlsegment.db import MySQLCredentials
from punchpipe.controlsegment.db import Flow, File, FileRelationship

def get_database_session():
    """Sets up a session to connect to the MariaDB punchpipe database"""
    credentials = MySQLCredentials.load("mysql-cred")
    engine = create_engine(
        f'mysql+pymysql://{credentials.user}:{credentials.password.get_secret_value()}@localhost/punchpipe')
    session = Session(engine)
    return session


@task
def update_file_state(session, file_id, new_state):
    session.query(File).where(File.file_id == file_id).update({"state": new_state})
    session.commit()