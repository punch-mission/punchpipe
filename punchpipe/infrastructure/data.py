from __future__ import annotations
from ndcube import NDCube
from astropy.io import fits
from astropy.wcs import WCS
import astropy.units as u
from typing import Union, Optional
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
from dateutil.parser import parse as parse_datetime


class PUNCHCalibration:
    pass

class PUNCHData:
    """PUNCH data object
    Allows for the input of a dictionary of NDCubes for storage and custom methods.
    Used to bundle multiple data sets together to pass through the PUNCH pipeline.

    See Also
    --------
    NDCube : Base container for the PUNCHData object

    Examples
    --------
    >>> from punchpipe.infrastructure.data import PUNCHData
    >>> from ndcube import NDCube

    >>> ndcube_obj = NDCube(data, wcs=wcs, uncertainty=uncertainty, meta=meta, unit=unit)
    >>> data_obj = {'default': ndcube_obj}

    >>> data = PUNCHData(data_obj)
    """

    def __init__(self, data_obj: Union[dict, NDCube, None]) -> None:
        """Initialize the PUNCHData object with either an
        empty NDCube object, or a provided NDCube / dictionary
        of NDCube objects

        Parameters
        ----------
        data_obj
            input data object

        Returns
        ----------
        None

        """

        if isinstance(data_obj, dict):
            self.cubes = data_obj
        elif isinstance(data_obj, NDCube):
            self.cubes = ("default", data_obj)
        elif data_obj is None:
            self.cubes = dict()
        else:
            print("Please specify either an NDCube object, or a dictionary of NDCube objects")

    @classmethod
    def from_fits(cls, filename: str) -> PUNCHData:
        with fits.open(filename) as hdul:
            data = hdul[0].data
            wcs = WCS(hdul[0].header)
            uncertainty = np.sqrt(np.abs(data))
            meta = dict()
            unit = u.ct  # counts
            ndcube_obj = NDCube(data, wcs=wcs, uncertainty=uncertainty, meta=meta, unit=unit)
            data_obj = {"wfi-starfield": ndcube_obj}
        return cls(data_obj)

    def write(self, filename: str, kind: str = "default") -> None:
        """Write to FITS file"""
        data = self.cubes[kind].data

        fits.writeto(filename, data)

    def plot(self, kind: str = "default") -> None:
        """Generate relevant plots to display or file"""
        self.cubes[kind].show()

    @property
    def observatory(self, kind: str = "default") -> str:
        """Observatory or Telescope name"""
        return self.cubes[kind].meta.get('obsrvtry', self.meta.get('telescop', "")).replace("_", " ")

    @property
    def instrument(self, kind: str = "default") -> str:
        """Instrument name"""
        return self.cubes[kind].meta.get('instrument', "").replace("_", " ")

    @property
    def detector(self, kind: str = "default") -> str:
        """Detector name"""
        return self.cubes[kind].meta.get('detector', "")

    @property
    def processing_level(self, kind: str = "default") -> int:
        """FITS processing level if present"""
        return self.cubes[kind].meta.get('lvl_num', None)

    @property
    def exposure_time(self, kind: str = "default") -> float:
        """Exposure time of the image"""
        if 'exptime' in self.cubes[kind].meta:
            return self.cubes[kind].meta['exptime']

    @property
    def date_obs(self, kind: str = "default") -> datetime:
        return parse_datetime(self.cubes[kind].meta.get("date-obs"))
