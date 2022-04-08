import os
from pytest import fixture
from punchpipe.infrastructure.data import PUNCHData


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
    pass


def test_generate_from_filename():
    pass


def test_generate_from_filenamelist():
    pass


def test_generate_from_filenamedict():
    pass


def test_generate_from_ndcube():
    pass


def test_generate_from_ndcubedict():
    pass


def test_write_data():
    pass


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