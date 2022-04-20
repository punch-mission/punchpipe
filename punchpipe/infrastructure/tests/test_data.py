import os
from pytest import fixture
from datetime import datetime
from punchpipe.infrastructure.data import PUNCHData, History, HistoryEntry

from ndcube import NDCube

import numpy as np


TESTDATA_DIR = os.path.dirname(__file__)
SAMPLE_FITS_PATH = os.path.join(TESTDATA_DIR, "L0_CL1_20211111070246_PUNCHData.fits")
SAMPLE_WRITE_PATH = os.path.join(TESTDATA_DIR, "write_test.fits")


@fixture
def sample_data():
    return PUNCHData.from_fits(SAMPLE_FITS_PATH)


@fixture
def write_data():
    return PUNCHData.write(SAMPLE_WRITE_PATH)


def test_sample_data_creation(sample_data):
    assert isinstance(sample_data, PUNCHData)


def test_generate_empty():
    pd = PUNCHData()
    assert isinstance(pd, PUNCHData)


def test_generate_from_filename():
    pd = PUNCHData(SAMPLE_FITS_PATH)
    assert isinstance(pd, PUNCHData)


def test_generate_from_filenamelist():
    fl_list = [SAMPLE_FITS_PATH, SAMPLE_FITS_PATH]
    pd = PUNCHData(fl_list)
    assert isinstance(pd, PUNCHData)


def test_generate_from_filenamedict():
    fl_dict = {"default": SAMPLE_FITS_PATH}
    pd = PUNCHData(fl_dict)
    assert isinstance(pd, PUNCHData)


def test_generate_from_ndcube():
    nd_obj = NDCube(np.zeros(1024,1024))
    pd = PUNCHData(nd_obj)
    assert isintance(pd, PUNCHData)


def test_generate_from_ndcubedict():
    nd_obj = NDCube(np.zeros(1024, 1024))
    data_obj = {"default": nd_obj}
    pd = PUNCHData(data_obj)
    assert isintance(pd, PUNCHData)


def test_write_data():
    pd = PUNCHData(SAMPLE_FITS_PATH)
    pd.write(SAMPLE_WRITE_PATH)
    # Check for writing to file? Read back in and compare?


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
