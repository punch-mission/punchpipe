from __future__ import annotations
from ndcube import NDCube
from ndcube.visualization import BasePlotter, PlotterDescriptor
from astropy.io import fits
from astropy.wcs import WCS
import astropy.units as u
from typing import Union, Optional
import numpy as np
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
    def from_fits(cls, inputs: Union[str, List[str], Dict[str, str]]) -> PUNCHData:
        """
        tries to infer from filename for str and List[str] what the keywords are. If it can't infer... Exception!
        which tells them to do it manually For the dict it uses the manually indicated keywords
        Parameters
        ----------
        inputs

        Returns
        -------

        """
        with fits.open(filename) as hdul:
            data = hdul[0].data
            meta = hdul[0].header
            wcs = WCS(hdul[0].header)
            uncertainty = hdul[1].data
            mask = hdul[2].data
            unit = u.ct  # counts
            ndcube_obj = NDCube(data, wcs=wcs, uncertainty=uncertainty, mask=mask, meta=meta, unit=unit)
            data_obj = {"wfi-starfield": ndcube_obj}
        return cls(data_obj)

    # Implement some other functions from dictionary

    def __getitem__(self, item) -> None:
        pass

    def __setitem__(self, key, value) -> None:
        pass

    def __delitem__(self, key) -> None:
        pass

    def clear(self) -> None:
        """remove all NDCubes"""
        pass

    def update(self, other: PUNCHData) -> None:
        """merge two PUNCHData objects"""
        pass

    def __contains__(self, item):
        pass

    def merge(self):
        pass

    def purge(self):
        # Just call the built in dictionary delitem function?
        pass

    def write(self, filename: str, kind: str = "default") -> None:
        """Write to FITS file"""
        data = self.cubes[kind].data
        uncert = self.cubes[kind].uncertainty
        mask = self.cubes[kind].mask
        meta = self.cubes[kind].meta
        wcs = self.cubes[kind].wcs

        hdu_data = fits.PrimaryHDU()
        hdu_data.data = data
        hdu_data.header = meta

        hdu_uncert = fits.ImageHDU()
        hdu_uncert.data = uncert

        hdu_mask = fits.ImageHDU()
        hdu_mask.data = mask

        hdul = fits.HDUList([hdu_data, hdu_uncert, hdu_mask])

        hdul.writeto(filename)

    def plot(self, kind: str = "default") -> None:
        """Generate relevant plots to display or file"""
        self.cubes[kind].show()

    def observatory(self, kind: str = "default") -> str:
        """Observatory or Telescope name"""
        return self.cubes[kind].meta.get('obsrvtry', self.meta.get('telescop', "")).replace("_", " ")

    def instrument(self, kind: str = "default") -> str:
        """Instrument name"""
        return self.cubes[kind].meta.get('instrument', "").replace("_", " ")

    def detector(self, kind: str = "default") -> str:
        """Detector name"""
        return self.cubes[kind].meta.get('detector', "")

    def processing_level(self, kind: str = "default") -> int:
        """FITS processing level if present"""
        return self.cubes[kind].meta.get('lvl_num', None)

    def exposure_time(self, kind: str = "default") -> float:
        """Exposure time of the image"""
        if 'exptime' in self.cubes[kind].meta:
            return self.cubes[kind].meta['exptime']

    def date_obs(self, kind: str = "default") -> datetime:
        return parse_datetime(self.cubes[kind].meta.get("date-obs"))

class HeaderTemplate:
    """

    """

    pass

class History:
    """
    
    """
    pass

class PUNCHPlotter(BasePlotter):
    """
    Custom PUNCHPlotter class
    """
    def plot(self):
        pass