import os

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from prefect import task
import yaml
from yaml.loader import FullLoader
from punchbowl.data import PUNCHData

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


@task
def load_pipeline_configuration(path: str) -> dict:
    with open(path) as f:
        config = yaml.load(f, Loader=FullLoader)
    # TODO: add validation
    return config


def write_file(data: PUNCHData, corresponding_file_db_entry, pipeline_config) -> None:
    output_filename = os.path.join(corresponding_file_db_entry.directory(pipeline_config['root']),
                                   corresponding_file_db_entry.filename())
    output_dir = os.path.dirname(output_filename)
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
    data.write(output_filename)
    corresponding_file_db_entry.state = "created"


def match_data_with_file_db_entry(data: PUNCHData, file_db_entry_list):
    # figure out which file_db_entry this corresponds to
    matching_entries = [file_db_entry for file_db_entry in file_db_entry_list
                        if file_db_entry.filename() == data.filename_base + ".fits"]
    if len(matching_entries) == 0:
        raise RuntimeError("There did not exist a file_db_entry for this result.")
    elif len(matching_entries) > 1:
        raise RuntimeError("There were many database entries matching this result. There should only be one.")
    else:
        return matching_entries[0]
