import os

import pandas as pd
import pytest

from punchpipe.cli import clean_replay

TEST_DIR = os.path.dirname(__file__)


def test_clean_replay():
    input_file = os.path.join(TEST_DIR, "data/test_replay.csv")
    result = clean_replay(input_file, write=False, timerange=None)

    assert isinstance(result, pd.DataFrame)
    assert len(result) != 0


def test_clean_replay_notime():
    input_file = os.path.join(TEST_DIR, "data/test_replay.csv")
    result = clean_replay(input_file, write=False, timerange=None)

    assert isinstance(result, pd.DataFrame)
    assert len(result) != 0


# TODO - other tests - empty request, fully connected requests, only one request

def test_clean_replay_empty():
    assert True


def test_clean_replay_one_request():
    assert True


def test_clean_replay_connected_requests():
    assert True
