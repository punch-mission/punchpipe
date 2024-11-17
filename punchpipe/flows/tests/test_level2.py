import os
from datetime import datetime

from freezegun import freeze_time
from prefect.logging import disable_run_logger
from prefect.testing.utilities import prefect_test_harness
from pytest_mock_resources import create_mysql_fixture

from punchpipe import __version__
from punchpipe.control.db import Base, File, Flow
from punchpipe.control.util import load_pipeline_configuration
from punchpipe.flows.level2 import (
    level2_construct_file_info,
    level2_construct_flow_info,
    level2_query_ready_files,
    level2_scheduler_flow,
)

TEST_DIR = os.path.dirname(__file__)


def session_fn(session):
    level0_file = File(level=0,
                       file_type='XX',
                       observatory='0',
                       state='created',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime(2023, 1, 1, 0, 0, 0))

    level1_file = File(level=1,
                       file_type="XX",
                       observatory='0',
                       state='created',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime(2023, 1, 1, 0, 0, 0))

    session.add(level0_file)
    session.add(level1_file)


db = create_mysql_fixture(Base, session_fn, session=True)


def test_level2_query_ready_files(db):
    with disable_run_logger():
        with freeze_time(datetime(2023, 1, 1, 0, 5, 0)) as frozen_datatime:  # noqa: F841
            pipeline_config = {'levels': {'level2_process_flow': {'schedule':
                                                                      {'latency': 3, 'window_duration_seconds': 3}}}}
            ready_file_ids = level2_query_ready_files.fn(db, pipeline_config)
            assert len(ready_file_ids) == 0


def test_level2_construct_file_info():
    pipeline_config_path = os.path.join(TEST_DIR, "punchpipe_config.yaml")
    pipeline_config = load_pipeline_configuration.fn(pipeline_config_path)

    level1_file = [File(level=0,
                       file_type='PT',
                       observatory='M',
                       state='created',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime.now())]
    constructed_file_info = level2_construct_file_info.fn(level1_file, pipeline_config)[0]
    assert constructed_file_info.level == 2
    assert constructed_file_info.file_type == level1_file[0].file_type
    assert constructed_file_info.observatory == level1_file[0].observatory
    assert constructed_file_info.file_version == "0.0.1"
    assert constructed_file_info.software_version == __version__
    assert constructed_file_info.date_obs == level1_file[0].date_obs
    assert constructed_file_info.polarization == level1_file[0].polarization
    assert constructed_file_info.state == "planned"


def test_level2_construct_flow_info():
    pipeline_config_path = os.path.join(TEST_DIR, "punchpipe_config.yaml")
    pipeline_config = load_pipeline_configuration.fn(pipeline_config_path)
    level1_file = [File(level="1",
                       file_type='XX',
                       observatory='0',
                       state='created',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime.now())]
    level2_file = level2_construct_file_info.fn(level1_file, pipeline_config)
    flow_info = level2_construct_flow_info.fn(level1_file, level2_file, pipeline_config)

    assert flow_info.flow_type == 'level2_process_flow'
    assert flow_info.state == "planned"
    assert flow_info.flow_level == "2"
    assert flow_info.priority == 7


def test_level2_scheduler_flow(db):
    pipeline_config_path = os.path.join(TEST_DIR, "punchpipe_config.yaml")
    with prefect_test_harness():
        level2_scheduler_flow(pipeline_config_path, db)
    results = db.query(Flow).where(Flow.state == 'planned').all()
    assert len(results) == 0


def test_level2_process_flow(db):
    pass
