import os
from datetime import datetime

import yaml
from ndcube import NDCube
from prefect import task
from prefect.variables import Variable
from prefect_sqlalchemy import SqlAlchemyConnector
from punchbowl.data import get_base_file_name, write_ndcube_to_fits, write_ndcube_to_jp2
from sqlalchemy import or_
from sqlalchemy.orm import Session
from yaml.loader import FullLoader

from punchpipe.control.db import File


def get_database_session():
    """Sets up a session to connect to the MariaDB punchpipe database"""
    credentials = SqlAlchemyConnector.load("mariadb-creds", _sync=True)
    engine = credentials.get_engine()
    session = Session(engine)
    return session


@task
def update_file_state(session, file_id, new_state):
    session.query(File).where(File.file_id == file_id).update({"state": new_state})
    session.commit()


@task
def load_pipeline_configuration(path: str = None) -> dict:
    if path is None:
        path = Variable.get("punchpipe_config", "punchpipe_config.yaml")
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

    layer = 0 if len(data.data.shape) > 2 else None
    write_ndcube_to_jp2(data, output_filename.replace(".fits", ".jp2"), layer=layer)


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


def get_files_in_time_window(level: str,
                             file_type: str,
                             obs_code: str,
                             start_time: datetime,
                             end_time: datetime,
                             session: Session | None) -> [File]:
    if session is None:
        get_database_session()

    return (((((session.query(File).filter(or_(File.state == "created", File.state == "progressed"))
            .filter(File.level == level))
            .filter(File.file_type == file_type))
            .filter(File.observatory == obs_code))
            .filter(File.date_obs > start_time))
            .filter(File.date_obs <= end_time).all())
