import os
from pytest import fixture
from punchpipe.infrastructure.data import PUNCHData


TESTDATA_DIR = os.path.dirname(__file__)
SAMPLE_FITS_PATH = os.path.join(TESTDATA_DIR, "L0_CL1_20211111070246.fits")


@fixture
def sample_data():
    return PUNCHData.from_fits(SAMPLE_FITS_PATH)


def test_sample_data_creation(sample_data):
    assert isinstance(sample_data, PUNCHData)

