import os
from datetime import datetime
from itertools import islice

import yaml
from ndcube import NDCube
from prefect.variables import Variable
from prefect_sqlalchemy import SqlAlchemyConnector
from punchbowl.data import get_base_file_name, write_ndcube_to_fits, write_ndcube_to_quicklook
from sqlalchemy import or_
from sqlalchemy.orm import Session
from yaml.loader import FullLoader

from punchpipe.control.db import File

DEFAULT_SCALING = (5e-13, 5e-11)

def get_database_session(get_engine=False):
    """Sets up a session to connect to the MariaDB punchpipe database"""
    credentials = SqlAlchemyConnector.load("mariadb-creds", _sync=True)
    engine = credentials.get_engine()
    session = Session(engine)

    if get_engine:
        return session, engine
    else:
        return session


def update_file_state(session, file_id, new_state):
    session.query(File).where(File.file_id == file_id).update({"state": new_state})
    session.commit()


def load_pipeline_configuration(path: str = None) -> dict:
    if path is None:
        path = Variable.get("punchpipe_config", "punchpipe_config.yaml")
    with open(path) as f:
        config = yaml.load(f, Loader=FullLoader)
    # TODO: add validation
    return config


def load_quicklook_scaling(level: str = None, product: str = None, obscode: str = None, path: str = None) -> (float, float):
    if path is None:
        path = Variable.get("punchpipe_config", "punchpipe_config.yaml")
    with open(path) as f:
        config = yaml.load(f, Loader=FullLoader)
    if "quicklook_scaling" in config:
        if level:
            level_data = config.get('quicklook_scaling', {}).get(level, {})
            if product and isinstance(level_data, dict):
                product_data = level_data.get(product, level_data.get('default'))
                if obscode == "4":
                    return product_data[1]
                else:
                    return product_data[0]
            if obscode == "4":
                return level_data.get('default')[1]
            else:
                return level_data.get('default')[0]
        else:
            return DEFAULT_SCALING
    else:
        return DEFAULT_SCALING


def write_file(data: NDCube, corresponding_file_db_entry, pipeline_config) -> None:
    output_filename = os.path.join(
        corresponding_file_db_entry.directory(pipeline_config["root"]), corresponding_file_db_entry.filename()
    )
    output_dir = os.path.dirname(output_filename)
    os.makedirs(output_dir, exist_ok=True)
    write_ndcube_to_fits(data, output_filename)
    corresponding_file_db_entry.state = "created"
    corresponding_file_db_entry.date_created = datetime.now()

    # TODO - Configure to write each layer separately?
    layer = 0 if len(data.data.shape) > 2 else None
    write_ndcube_to_quicklook(data, output_filename.replace(".fits", ".jp2"), layer=layer)
    return output_filename


def match_data_with_file_db_entry(data: NDCube, file_db_entry_list):
    # figure out which file_db_entry this corresponds to
    matching_entries = [
        file_db_entry
        for file_db_entry in file_db_entry_list
        if file_db_entry.filename() == get_base_file_name(data) + ".fits"
    ]
    if len(matching_entries) == 0:
        for file in file_db_entry_list:
            raise RuntimeError(f"There did not exist a file_db_entry for this result: "
                               f"result={get_base_file_name(data)}. Candidate: {file.filename()}")
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


def batched(iterable, n):
    # batched('ABCDEFG', 3) → ABC DEF G
    # This is basically itertools.batched, but that only exists in Python >= 3.12
    if n < 1:
        raise ValueError('n must be at least one')
    iterator = iter(iterable)
    while batch := tuple(islice(iterator, n)):
        yield batch
