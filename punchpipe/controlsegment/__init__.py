from datetime import datetime

import pytest
import numpy as np
from astropy.nddata import StdDevUncertainty
from astropy.wcs import WCS
from punchbowl.data import NormalizedMetadata, PUNCHData

from punchpipe.controlsegment.db import Base, Flow, File
from punchpipe.controlsegment.processor import generic_process_flow_logic
from punchpipe.controlsegment.util import match_data_with_file_db_entry


@pytest.fixture()
def sample_punchdata(shape=(50, 50), level=0):
    data = np.random.random(shape)
    uncertainty = StdDevUncertainty(np.sqrt(np.abs(data)))
    wcs = WCS(naxis=2)
    wcs.wcs.ctype = "HPLN-ARC", "HPLT-ARC"
    wcs.wcs.cunit = "deg", "deg"
    wcs.wcs.cdelt = 0.1, 0.1
    wcs.wcs.crpix = 0, 0
    wcs.wcs.crval = 1, 1
    wcs.wcs.cname = "HPC lon", "HPC lat"

    meta = NormalizedMetadata({"LEVEL": level})
    return PUNCHData(data=data, uncertainty=uncertainty, wcs=wcs, meta=meta)


def test_match_data_with_file_db_entry_fails_on_empty_list(sample_punchdata):
    file_db_entry_list = []
    with pytest.raises(RuntimeError):
        match_data_with_file_db_entry(sample_punchdata, file_db_entry_list)


def test_match_data_with_file_db_entry(sample_punchdata):
    file_db_entry_list = [File(level=1,
                               file_type='XX',
                               observatory='Y',
                               file_version='0',
                               software_version='0',
                               date_created=datetime.now(),
                               date_obs=datetime.now(),
                               date_beg=datetime.now(),
                               date_end=datetime.now(),
                               polarization='ZZ',
                               state='created',
                               processing_flow=0),
                          File(level=100,
                               file_type='XX',
                               observatory='Y',
                               file_version='0',
                               software_version='0',
                               date_created=datetime.now(),
                               date_obs=datetime.now(),
                               date_beg=datetime.now(),
                               date_end=datetime.now(),
                               polarization='ZZ',
                               state='created',
                               processing_flow=0)
                          ]
    output = match_data_with_file_db_entry(sample_punchdata, file_db_entry_list)
    assert len(output) == 1
    assert output == file_db_entry_list[0]