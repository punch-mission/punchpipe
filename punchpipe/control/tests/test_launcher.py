import os
from datetime import datetime

from freezegun import freeze_time
from prefect.logging import disable_run_logger
from prefect.testing.utilities import prefect_test_harness
from pytest_mock_resources import create_mysql_fixture

from punchpipe.control.db import Base, File, Flow
from punchpipe.control.launcher import (
    count_running_flows,
    escalate_long_waiting_flows,
    filter_for_launchable_flows,
    gather_planned_flows,
)
from punchpipe.control.util import load_pipeline_configuration

TEST_DIR = os.path.dirname(__file__)


def session_fn(session):
    level0_file = File(level=0,
                       file_type='XX',
                       observatory='0',
                       state='created',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime.now())

    level1_file = File(level=1,
                       file_type="XX",
                       observatory='0',
                       state='created',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime.now())

    level0_planned_flow = Flow(flow_id=1,
                               flow_level=0,
                              flow_type='level0_process_flow',
                              state='planned',
                              creation_time=datetime(2023, 2, 2, 0, 0, 0),
                              priority=5)

    level1_planned_flow = Flow(flow_id=2,
                               flow_level=1,
                              flow_type='level1_process_flow',
                              state='planned',
                              creation_time=datetime(2023, 2, 2, 0, 0, 0),
                              priority=2)

    level1_planned_flow2 = Flow(flow_id=3,
                               flow_level=1,
                              flow_type='level1_process_flow',
                              state='planned',
                              creation_time=datetime(2023, 2, 2, 0, 0, 0),
                              priority=100)

    session.add(level0_file)
    session.add(level1_file)
    session.add(level0_planned_flow)
    session.add(level1_planned_flow)
    session.add(level1_planned_flow2)


db = create_mysql_fixture(Base, session_fn, session=True)
db_empty = create_mysql_fixture(Base, session=True)


def test_gather_queued_flows(db):
    planned_ids = gather_planned_flows.fn(db)
    assert len(planned_ids) == 3


def test_count_running_flows(db):
    running_count = count_running_flows.fn(db)
    assert running_count == 0


def test_escalate_long_waiting_flows(db):
    pipeline_config_path = os.path.join(TEST_DIR, "punchpipe_config.yaml")
    pipeline_config = load_pipeline_configuration.fn(pipeline_config_path)

    with freeze_time(datetime(2023, 2, 2, 0, 0, 0)) as frozen_datetime:
        escalate_long_waiting_flows.fn(db, pipeline_config)
        assert db.query(Flow).where(Flow.flow_id == 1).one().priority == 5

        frozen_datetime.move_to(datetime(2023, 2, 2, 0, 0, 31))
        escalate_long_waiting_flows.fn(db, pipeline_config)
        assert db.query(Flow).where(Flow.flow_id == 1).one().priority == 10

        frozen_datetime.move_to(datetime(2024, 2, 2, 0, 0, 0))
        escalate_long_waiting_flows.fn(db, pipeline_config)
        assert db.query(Flow).where(Flow.flow_id == 1).one().priority == 30


def test_filter_for_launchable_flows(db):
    with prefect_test_harness(), disable_run_logger():
        planned_ids = gather_planned_flows.fn(db)
        running_count = count_running_flows.fn(db)
        max_flows_running = 30
        ready_to_launch_flows = filter_for_launchable_flows.fn(planned_ids, running_count, max_flows_running)
        assert len(ready_to_launch_flows) == 3


def test_filter_for_launchable_flows_with_max_of_1(db):
    with prefect_test_harness(), disable_run_logger():
        planned_ids = gather_planned_flows.fn(db)
        running_count = count_running_flows.fn(db)
        max_flows_running = 1
        ready_to_launch_flows = filter_for_launchable_flows.fn(planned_ids, running_count, max_flows_running)
        assert len(ready_to_launch_flows) == 1
        assert ready_to_launch_flows[0] == 3


def test_filter_for_launchable_flows_with_max_of_0(db):
    with prefect_test_harness(), disable_run_logger():
        planned_ids = gather_planned_flows.fn(db)
        running_count = count_running_flows.fn(db)
        max_flows_running = 0
        ready_to_launch_flows = filter_for_launchable_flows.fn(planned_ids, running_count, max_flows_running)
        assert len(ready_to_launch_flows) == 0


def test_filter_for_launchable_flows_with_empty_db(db_empty):
    with prefect_test_harness(), disable_run_logger():
        planned_ids = gather_planned_flows.fn(db_empty)
        running_count = count_running_flows.fn(db_empty)
        max_flows_running = 30
        ready_to_launch_flows = filter_for_launchable_flows.fn(planned_ids, running_count, max_flows_running)
        assert len(ready_to_launch_flows) == 0


def test_launch_ready_flows():
    pass
