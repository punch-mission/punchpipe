from datetime import datetime

from pytest_mock_resources import create_mysql_fixture

from punchpipe.controlsegment.db import Base, Flow, File


def session_fn(session):
    session.add(Flow(flow_level=1,
                     flow_type="test",
                     state="tester",
                     creation_time=datetime.now(),
                     priority=1))


db = create_mysql_fixture(Base, session_fn, session=True)


def test_first(db):
    execute = db.execute("SELECT state FROM flows WHERE flow_level=1;")
    result = [row[0] for row in execute]
    assert len(result) == 1
    assert result == ['tester']


def test_second(db):
    execute = db.execute("SELECT state FROM flows WHERE flow_level=1;")
    result = [row[0] for row in execute]
    assert len(result) == 1
    assert result == ['tester']

