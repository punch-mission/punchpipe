import os
from pytest import fixture
from datetime import datetime
from punchpipe.infrastructure.data import PUNCHData, History, HistoryEntry


TESTDATA_DIR = os.path.dirname(__file__)
SAMPLE_FITS_PATH = os.path.join(TESTDATA_DIR, "L0_CL1_20211111070246.fits")


@fixture
def sample_data():
    return PUNCHData.from_fits(SAMPLE_FITS_PATH)


def test_sample_data_creation(sample_data):
    assert isinstance(sample_data, PUNCHData)


@fixture
def empty_history():
    return History()


def test_history_add_one(empty_history):
    entry = HistoryEntry(datetime.now(), "test", "dummy")
    assert len(empty_history) == 0
    empty_history.add_entry(entry)
    assert len(empty_history) == 1
    assert empty_history.most_recent().source == "test"
    assert empty_history.most_recent().comment == "dummy"
    assert empty_history.most_recent() == empty_history[-1]
    empty_history.clear()
    assert len(empty_history) == 0
