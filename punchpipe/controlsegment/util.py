import os

import yaml
from prefect import task
from sqlalchemy.orm import Session
from yaml.loader import FullLoader
from prefect_sqlalchemy.credentials import DatabaseCredentials
from ndcube import NDCube
from punchbowl.data import write_ndcube_to_fits, get_base_file_name

from punchpipe.controlsegment.db import File


def get_database_session():
    """Sets up a session to connect to the MariaDB punchpipe database"""
    credentials = DatabaseCredentials.load("mariadb-creds")
    engine = credentials.get_engine()
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


def write_file(data: NDCube, corresponding_file_db_entry, pipeline_config) -> None:
    output_filename = os.path.join(
        corresponding_file_db_entry.directory(pipeline_config["root"]), corresponding_file_db_entry.filename()
    )
    output_dir = os.path.dirname(output_filename)
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
    write_ndcube_to_fits(data, output_filename)
    corresponding_file_db_entry.state = "created"


def match_data_with_file_db_entry(data: NDCube, file_db_entry_list):
    # figure out which file_db_entry this corresponds to
    matching_entries = [
        file_db_entry
        for file_db_entry in file_db_entry_list
        if file_db_entry.filename() == get_base_file_name(data) + ".fits"
    ]
    if len(matching_entries) == 0:
        raise RuntimeError(f"There did not exist a file_db_entry for this result: result={get_base_file_name(data)}.")
    elif len(matching_entries) > 1:
        raise RuntimeError("There were many database entries matching this result. There should only be one.")
    else:
        return matching_entries[0]
