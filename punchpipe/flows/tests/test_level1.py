from datetime import datetime

from pytest_mock_resources import create_mysql_fixture

from punchpipe.controlsegment.db import Base, Flow, File
from punchpipe.flows.level1 import level1_query_ready_files


def session_fn(session):
    session.add(File(level=0,
                     file_type='XX',
                     observatory='0',
                     state='created',
                     file_version='none',
                     software_version='none',
                     date_obs=datetime.now()))


db = create_mysql_fixture(Base, session_fn, session=True)


def test_query_ready_files(db):
    ready_file_ids = level1_query_ready_files.fn(db)
    assert len(ready_file_ids) == 1
