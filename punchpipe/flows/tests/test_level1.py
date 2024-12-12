import os
from datetime import datetime, timedelta

import pytest
from prefect.testing.utilities import prefect_test_harness
from pytest_mock_resources import create_mysql_fixture

from punchpipe import __version__
from punchpipe.control.db import Base, File, Flow
from punchpipe.control.util import load_pipeline_configuration
from punchpipe.flows.level1 import (
    level1_construct_file_info,
    level1_construct_flow_info,
    level1_query_ready_files,
    level1_scheduler_flow,
)

TEST_DIR = os.path.dirname(__file__)

@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    with prefect_test_harness():
        yield

def session_fn(session):
    level0_file = File(level="0",
                       file_type='PM',
                       observatory='0',
                       state='created',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime.now())

    level1_file = File(level="1",
                       file_type="PM",
                       observatory='0',
                       state='created',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime.now())

    psf_model = File(level="1",
                       file_type="RM",
                       observatory='0',
                       state='created',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime.now()-timedelta(days=1))

    quartic_fit_coeffs = File(level="1",
                       file_type="FQ",
                       observatory='0',
                       state='created',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime.now()-timedelta(days=1))

    session.add(level0_file)
    session.add(level1_file)
    session.add(psf_model)
    session.add(quartic_fit_coeffs)

db = create_mysql_fixture(Base, session_fn, session=True)


def test_query_ready_files(db):
    pipeline_config_path = os.path.join(TEST_DIR, "punchpipe_config.yaml")
    pipeline_config = load_pipeline_configuration.fn(pipeline_config_path)
    ready_file_ids = level1_query_ready_files.fn(db, pipeline_config)
    assert len(ready_file_ids) == 1


def test_level1_construct_file_info():
    pipeline_config_path = os.path.join(TEST_DIR, "punchpipe_config.yaml")
    pipeline_config = load_pipeline_configuration.fn(pipeline_config_path)
    level0_file = [File(level="0",
                       file_type='PM',
                       observatory='0',
                       state='created',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime.now())]
    constructed_file_info = level1_construct_file_info.fn(level0_file, pipeline_config)[0]
    assert constructed_file_info.level == "1"
    assert constructed_file_info.file_type == level0_file[0].file_type
    assert constructed_file_info.observatory == level0_file[0].observatory
    assert constructed_file_info.file_version == "0.0.1"
    assert constructed_file_info.software_version == __version__
    assert constructed_file_info.date_obs == level0_file[0].date_obs
    assert constructed_file_info.polarization == level0_file[0].polarization
    assert constructed_file_info.state == "planned"


def test_level1_construct_flow_info(db):
    pipeline_config_path = os.path.join(TEST_DIR, "punchpipe_config.yaml")
    pipeline_config = load_pipeline_configuration.fn(pipeline_config_path)
    level0_file = [File(level="0",
                       file_type='PM',
                       observatory='0',
                       state='created',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime.now())]
    level1_file = level1_construct_file_info.fn(level0_file, pipeline_config)
    flow_info = level1_construct_flow_info.fn(level0_file, level1_file, pipeline_config, session=db)

    assert flow_info.flow_type == 'level1_process_flow'
    assert flow_info.state == "planned"
    assert flow_info.flow_level == "1"
    assert flow_info.priority == 6


def test_level1_scheduler_flow(db):
    pipeline_config_path = os.path.join(TEST_DIR, "punchpipe_config.yaml")
    with prefect_test_harness():
        level1_scheduler_flow(pipeline_config_path, db)
    results = db.query(Flow).where(Flow.state == 'planned').all()
    assert len(results) == 1


def test_level1_process_flow(db):
    pass
