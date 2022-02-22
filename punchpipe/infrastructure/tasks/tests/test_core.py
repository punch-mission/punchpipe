import numpy as np
from pytest import fixture
from controlsegment.data import DataObject


class TestDataObject(DataObject):
    def __init__(self, data: np.array) -> None:
        self.data = data


@fixture
def test_data_object():
    return TestDataObject(np.zeros(100))


def test_create_data_object(test_data_object):
    assert test_data_object.data.shape[0] == 100
    assert np.all(test_data_object.data == np.zeros(100))