from datetime import datetime
import json
import os
import shutil

import pytest
from prefect import flow, task
from prefect.testing.utilities import prefect_test_harness
from pytest_mock_resources import create_mysql_fixture
import numpy as np
from astropy.nddata import StdDevUncertainty
from astropy.wcs import WCS
from punchbowl.data import NormalizedMetadata, PUNCHData, PUNCH_REQUIRED_META_FIELDS

from punchpipe.controlsegment.db import Base, Flow, File
from punchpipe.controlsegment.processor import generic_process_flow_logic
from punchpipe.controlsegment.util import match_data_with_file_db_entry

TESTDATA_DIR = os.path.dirname(__file__)


def session_fn(session):
    level0_file = File(file_id=1,
                       level=0,
                       file_type='XX',
                       observatory='0',
                       state='created',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime.now(),
                       processing_flow=0)

    level1_file = File(file_id=2,
                       level=1,
                       file_type="XX",
                       observatory='0',
                       state='planned',
                       file_version='none',
                       software_version='none',
                       date_obs=datetime(2023, 1, 1, 0, 0, 1),
                       processing_flow=1)

    level0_planned_flow = Flow(flow_id=1,
                               flow_level=0,
                              flow_type='level0_process_flow',
                              state='planned',
                              creation_time=datetime(2023, 2, 2, 0, 0, 0),
                              priority=5,
                              call_data=json.dumps({}))

    level1_planned_flow = Flow(flow_id=2,
                               flow_level=1,
                              flow_type='level1_process_flow',
                              state='planned',
                              creation_time=datetime(2023, 2, 2, 0, 0, 0),
                              priority=2,
                              call_data=json.dumps({}))

    level1_planned_flow2 = Flow(flow_id=3,
                               flow_level=1,
                              flow_type='level1_process_flow',
                              state='planned',
                              creation_time=datetime(2023, 2, 2, 0, 0, 0),
                              priority=100,
                              call_data=json.dumps({}))

    level2_planned_flow = Flow(flow_id=4,
                                flow_level=2,
                              flow_type='level2_process_flow',
                              state='planned',
                              creation_time=datetime(2023, 2, 2, 0, 0, 0),
                              priority=1,
                              call_data=json.dumps({}))

    session.add(level0_file)
    session.add(level1_file)
    session.add(level0_planned_flow)
    session.add(level1_planned_flow)
    session.add(level1_planned_flow2)
    session.add(level2_planned_flow)


db = create_mysql_fixture(Base, session_fn, session=True)
db_empty = create_mysql_fixture(Base, session=True)

@flow
def empty_core_flow():
    return []

@flow
def empty_flow(flow_id: int, pipeline_config_path=TESTDATA_DIR+"/config.yaml", session=None):
    generic_process_flow_logic(flow_id, empty_core_flow, pipeline_config_path, session=session)


def test_generic_process_flow_fails_on_empty_db(db_empty):
    with pytest.raises(Exception):
        with prefect_test_harness():
            empty_flow(1, session=db_empty)


def test_simple_generic_process_flow_unreported(db):
    level1_file = db.query(File).where(File.file_id == 2).one()
    assert level1_file.state == "planned"
    del level1_file

    with prefect_test_harness():
        empty_flow(1, session=db)

    level1_file = db.query(File).where(File.file_id == 2).one()
    assert level1_file.state == "unreported"
    del level1_file


@flow
def blank_core_flow():
    data = np.random.random((50, 50))
    uncertainty = StdDevUncertainty(np.sqrt(np.abs(data)))
    wcs = WCS(naxis=2)
    wcs.wcs.ctype = "HPLN-ARC", "HPLT-ARC"
    wcs.wcs.cunit = "deg", "deg"
    wcs.wcs.cdelt = 0.1, 0.1
    wcs.wcs.crpix = 0, 0
    wcs.wcs.crval = 1, 1
    wcs.wcs.cname = "HPC lon", "HPC lat"

    meta = NormalizedMetadata({"LEVEL": str(1),
                               'OBSRVTRY': '0',
                               'TYPECODE': 'XX',
                               'DATE-OBS': str(datetime(2023, 1, 1, 0, 0, 1)),
                               'BLANK': True},
                              required_fields=PUNCH_REQUIRED_META_FIELDS)
    output = PUNCHData(data=data, uncertainty=uncertainty, wcs=wcs, meta=meta)

    return [output]


@flow
def blank_flow(flow_id: int, pipeline_config_path=TESTDATA_DIR+"/config.yaml", session=None):
    generic_process_flow_logic(flow_id, blank_core_flow, pipeline_config_path, session=session)


def test_simple_generic_process_flow_blank_return(db):
    level1_file = db.query(File).where(File.file_id == 2).one()
    assert level1_file.state == "planned"
    del level1_file

    with prefect_test_harness():
        blank_flow(1, session=db)

    level1_file = db.query(File).where(File.file_id == 2).one()
    assert level1_file.state == "blank"
    del level1_file


@flow
def normal_core_flow():
    data = np.random.random((50, 50))
    uncertainty = StdDevUncertainty(np.sqrt(np.abs(data)))
    wcs = WCS(naxis=2)
    wcs.wcs.ctype = "HPLN-ARC", "HPLT-ARC"
    wcs.wcs.cunit = "deg", "deg"
    wcs.wcs.cdelt = 0.1, 0.1
    wcs.wcs.crpix = 0, 0
    wcs.wcs.crval = 1, 1
    wcs.wcs.cname = "HPC lon", "HPC lat"

    meta = NormalizedMetadata({"LEVEL": str(1),
                               'OBSRVTRY': '0',
                               'TYPECODE': 'XX',
                               'DATE-OBS': str(datetime(2023, 1, 1, 0, 0, 1)),
                               'BLANK': False},
                              required_fields=PUNCH_REQUIRED_META_FIELDS)
    output = PUNCHData(data=data, uncertainty=uncertainty, wcs=wcs, meta=meta)

    return [output]


@flow
def normal_flow(flow_id: int, pipeline_config_path=TESTDATA_DIR+"/config.yaml", session=None):
    generic_process_flow_logic(flow_id, normal_core_flow, pipeline_config_path, session=session)


def test_simple_generic_process_flow_normal_return(db):
    os.mkdir("./test_results/")

    level1_file = db.query(File).where(File.file_id == 2).one()
    assert level1_file.state == "planned"
    del level1_file

    with prefect_test_harness():
        normal_flow(1, session=db)

    level1_file = db.query(File).where(File.file_id == 2).one()
    assert level1_file.state == "created"
    output_filename = os.path.join(level1_file.directory("./test_results/"), level1_file.filename())
    del level1_file

    assert os.path.isfile(output_filename)
    shutil.rmtree("./test_results/", ignore_errors=True)