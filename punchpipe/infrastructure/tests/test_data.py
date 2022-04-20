import os
from pytest import fixture
from punchpipe.infrastructure.data import PUNCHData

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


# Tests of PUNCHData object generation


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


# Tests of PUNCHData dictionary manipulation


def test_cubes_delete():
    pass


def test_cubes_clear():
    pass


def test_cubes_update():
    pass


# Tests of PUNCHData object writing


def test_gen_filename():
    pass


def test_write():
    pass


def test_write_png():
    pass