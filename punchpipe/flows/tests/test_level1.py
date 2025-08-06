import os
from datetime import UTC, datetime, timedelta

import pytest
from prefect.testing.utilities import prefect_test_harness
from pytest_mock_resources import create_mysql_fixture

from punchpipe import __version__
from punchpipe.control.db import Base, File, Flow
from punchpipe.control.util import load_pipeline_configuration
from punchpipe.flows.level1 import (
    level1_early_construct_file_info,
    level1_early_construct_flow_info,
    level1_early_query_ready_files,
    level1_early_scheduler_flow,
    level1_late_construct_file_info,
    level1_late_construct_flow_info,
    level1_late_query_ready_files,
    level1_late_scheduler_flow,
)

TEST_DIR = os.path.dirname(__file__)

@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    with prefect_test_harness():
        yield

def session_fn(session):
    level0_file = File(level="0",
                       file_type='PM',
                       observatory='2',
                       state='created',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime.now(UTC))

    level1_file = File(level="1",
                       file_type="PM",
                       observatory='2',
                       state='created',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime.now(UTC))

    level1_x_file = File(level="1",
                       file_type="XM",
                       observatory='2',
                       state='created',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime.now(UTC))

    psf_model = File(level="1",
                       file_type="RM",
                       observatory='2',
                       state='created',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime.now(UTC)-timedelta(days=1))

    quartic_fit_coeffs = File(level="1",
                       file_type="FQ",
                       observatory='2',
                       state='created',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime.now(UTC)-timedelta(days=1))

    vignetting_function = File(level="1",
                       file_type="GM",
                       observatory='2',
                       state='created',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime.now(UTC)-timedelta(days=1))

    stray_light_before = File(level="1",
                       file_type="SM",
                       observatory='2',
                       state='created',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime.now(UTC)-timedelta(hours=12))

    stray_light_after = File(level="1",
                       file_type="SM",
                       observatory='2',
                       state='created',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime.now(UTC)+timedelta(hours=12))

    distortion = File(level="1",
                       file_type="DS",
                       observatory='2',
                       state='created',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime.now(UTC)-timedelta(days=1))

    mask_file = File(level="1",
                       file_type="MS",
                       observatory='2',
                       state='created',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime.now(UTC)-timedelta(days=1))

    session.add(level0_file)
    session.add(level1_file)
    session.add(level1_x_file)
    session.add(psf_model)
    session.add(quartic_fit_coeffs)
    session.add(vignetting_function)
    session.add(distortion)
    session.add(stray_light_before)
    session.add(stray_light_after)
    session.add(mask_file)

db = create_mysql_fixture(Base, session_fn, session=True)


def test_level1_early_query_ready_files(db, prefect_test_fixture):
    pipeline_config_path = os.path.join(TEST_DIR, "punchpipe_config.yaml")
    pipeline_config = load_pipeline_configuration(pipeline_config_path)
    ready_file_ids = level1_early_query_ready_files(db, pipeline_config)
    assert len(ready_file_ids) == 1


def test_level1_late_query_ready_files(db, prefect_test_fixture):
    pipeline_config_path = os.path.join(TEST_DIR, "punchpipe_config.yaml")
    pipeline_config = load_pipeline_configuration(pipeline_config_path)
    ready_file_ids = level1_late_query_ready_files(db, pipeline_config)
    assert len(ready_file_ids) == 1


def test_level1_early_construct_file_info():
    pipeline_config_path = os.path.join(TEST_DIR, "punchpipe_config.yaml")
    pipeline_config = load_pipeline_configuration(pipeline_config_path)
    level0_file = [File(level="0",
                       file_type='PM',
                       observatory='2',
                       state='created',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime.now(UTC))]
    constructed_file_info = level1_early_construct_file_info(level0_file, pipeline_config)[0]
    assert constructed_file_info.level == "1"
    assert constructed_file_info.file_type == 'X' + level0_file[0].file_type[1:]
    assert constructed_file_info.observatory == level0_file[0].observatory
    assert constructed_file_info.file_version == "0.0.1"
    assert constructed_file_info.software_version == __version__
    assert constructed_file_info.date_obs == level0_file[0].date_obs
    assert constructed_file_info.polarization == level0_file[0].polarization
    assert constructed_file_info.state == "planned"


def test_level1_early_construct_file_info_clear():
    pipeline_config_path = os.path.join(TEST_DIR, "punchpipe_config.yaml")
    pipeline_config = load_pipeline_configuration(pipeline_config_path)
    level0_file = [File(level="0",
                       file_type='CR',
                       observatory='2',
                       state='created',
                       file_version='none',
                       software_version='none',
                       polarization='C',
                       date_obs=datetime.now(UTC))]
    q_file, x_file = level1_early_construct_file_info(level0_file, pipeline_config)
    assert x_file.file_type == 'X' + level0_file[0].file_type[1:]
    assert q_file.file_type == 'Q' + level0_file[0].file_type[1:]
    for file in q_file, x_file:
        assert file.level == "1"
        assert file.observatory == level0_file[0].observatory
        assert file.file_version == "0.0.1"
        assert file.software_version == __version__
        assert file.date_obs == level0_file[0].date_obs
        assert file.polarization == level0_file[0].polarization
        assert file.state == "planned"


def test_level1_late_construct_file_info():
    pipeline_config_path = os.path.join(TEST_DIR, "punchpipe_config.yaml")
    pipeline_config = load_pipeline_configuration(pipeline_config_path)
    input_file = [File(level="0",
                       file_type='XM',
                       observatory='2',
                       state='created',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime.now(UTC))]
    constructed_file_info = level1_late_construct_file_info(input_file, pipeline_config)[0]
    assert constructed_file_info.level == "1"
    assert constructed_file_info.file_type == 'P' + input_file[0].file_type[1:]
    assert constructed_file_info.observatory == input_file[0].observatory
    assert constructed_file_info.file_version == "0.0.1"
    assert constructed_file_info.software_version == __version__
    assert constructed_file_info.date_obs == input_file[0].date_obs
    assert constructed_file_info.polarization == input_file[0].polarization
    assert constructed_file_info.state == "planned"


def test_level1_early_construct_flow_info(db, prefect_test_fixture):
    pipeline_config_path = os.path.join(TEST_DIR, "punchpipe_config.yaml")
    pipeline_config = load_pipeline_configuration(pipeline_config_path)
    level0_file = [File(level="0",
                       file_type='PM',
                       observatory='2',
                       state='created',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime.now(UTC))]
    level1_file = level1_early_construct_file_info(level0_file, pipeline_config)
    flow_info = level1_early_construct_flow_info(level0_file, level1_file, pipeline_config, session=db)

    assert flow_info.flow_type == 'level1_early'
    assert flow_info.state == "planned"
    assert flow_info.flow_level == "1"
    assert flow_info.priority == 6


def test_level1_late_construct_flow_info(db, prefect_test_fixture):
    pipeline_config_path = os.path.join(TEST_DIR, "punchpipe_config.yaml")
    print(pipeline_config_path)
    pipeline_config = load_pipeline_configuration(pipeline_config_path)
    level0_file = [File(level="0",
                       file_type='XM',
                       observatory='2',
                       state='created',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime.now(UTC))]
    level1_file = level1_late_construct_file_info(level0_file, pipeline_config)
    print(pipeline_config)
    print(pipeline_config['flows'])
    flow_info = level1_late_construct_flow_info(level0_file, level1_file, pipeline_config, session=db)

    assert flow_info.flow_type == 'level1_late'
    assert flow_info.state == "planned"
    assert flow_info.flow_level == "1"
    assert flow_info.priority == 6


def test_level1_early_scheduler_flow(db):
    pipeline_config_path = os.path.join(TEST_DIR, "punchpipe_config.yaml")
    with prefect_test_harness():
        level1_early_scheduler_flow(pipeline_config_path, db)
    results = db.query(Flow).where(Flow.state == 'planned').all()
    assert len(results) == 1


def test_level1_late_scheduler_flow(db):
    pipeline_config_path = os.path.join(TEST_DIR, "punchpipe_config.yaml")
    with prefect_test_harness():
        level1_late_scheduler_flow(pipeline_config_path, db)
    results = db.query(Flow).where(Flow.state == 'planned').all()
    assert len(results) == 1


def test_level1_process_flow(db):
    pass
