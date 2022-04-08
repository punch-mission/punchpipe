from __future__ import annotations

from collections import namedtuple
from datetime import datetime
from typing import Union, Optional, List, Dict

import astropy.units as u
import matplotlib
import numpy as np
from astropy.io import fits
from astropy.nddata import StdDevUncertainty
from astropy.wcs import WCS
from dateutil.parser import parse as parse_datetime
from ndcube import NDCube
from ndcube.visualization import BasePlotter


class PUNCHCalibration:
    pass

# TODO - pull into develop branch once done


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
            raise Exception("Please specify either an NDCube object, or a dictionary of NDCube objects")

    @classmethod
    def from_fits(cls, inputs: Union[str, List[str], Dict[str, str]]) -> PUNCHData:
        """
        Populates a PUNCHData object from specified FITS files.
        Specify a filename string, a list of filename strings, or a dictionary of keys and filenames

        Parameters
        ----------
        inputs
            input from which to generate a PUNCHData object
            (filename string, a list of filename strings, or a dictionary of keys and filenames)

        Returns
        -------
        PUNCHData object

        """

        if type(inputs) is str:
            files = {"default": inputs}

        elif type(inputs) is list:
            files = {}
            for file in inputs:
                if type(inputs[file]) is str:
                    files[file] = file
                else:
                    raise Exception("PUNCHData objects are generated with a list of filename strings.")

        elif type(inputs) is dict:
            files = {}
            for key in inputs:
                if type(inputs[key]) is str:
                    files[key] = inputs[key]
                else:
                    raise Exception("PUNCHData objects are generated with a dictionary of keys and string filenames.")

        else:
            raise Exception("PUNCHData objects are generated with a filename string, a list of filename strings, "
                            "or a dictionary of keys and filenames")

        data_obj = {}

        for key in files:
            with fits.open(files[key]) as hdul:
                data = hdul[0].data
                meta = hdul[0].header
                wcs = WCS(hdul[0].header)
                uncertainty = StdDevUncertainty(hdul[1].data)
                unit = u.ct  # counts
                ndcube_obj = NDCube(data, wcs=wcs, uncertainty=uncertainty, meta=meta, unit=unit)
                data_obj[key] = ndcube_obj

        return cls(data_obj)

    def weight(self, kind: str = "default") -> np.ndarray:
        """
        Generate a corresponding weight map from the uncertainty array

        Parameters
        ----------
        kind
            specified element of the PUNCHData object to generate weights

        Returns
        -------
        weight
            weight map computed from uncertainty array

        """

        return 1./self.cubes[kind].uncertainty.array

    def __contains__(self, kind) -> bool:
        return kind in self.cubes

    def __getitem__(self, kind) -> NDCube:
        return self.cubes[kind]

    def __setitem__(self, kind, data) -> None:
        if type(data) is NDCube:
            self.cubes[kind] = data
        else:
            raise Exception("PUNCHData entries must contain NDCube objects.")

    def __delitem__(self, kind) -> None:
        del self.cubes[kind]

    def clear(self) -> None:
        """remove all NDCubes"""
        self.cubes.clear()

    def update(self, other: PUNCHData) -> None:
        """merge two PUNCHData objects"""
        self.cubes.update(other)

    def generate_filename(self, kind: str = "default") -> str:
        """
        Dynamically generate a filename for the given data product, using the format 'Ln_ttO_yyyymmddhhmmss'
        Parameters
        ----------
        kind
            specified element of the PUNCHData object to write to file

        Returns
        -------
        filename
            output filename string

        """
        # TODO - Lookup table for type_codes / matching with metadata (or just specify as FITS keyword?)
        observatory = self.cubes[kind].meta['OBSRVTRY']
        file_level = self.cubes[kind].meta['lvl_num']
        type_code = self.cubes[kind].meta['type_code']
        date_obs = self.cubes[kind].meta['DATE-OBS']
        time_obs = self.cubes[kind].meta['TIME-OBS']

        date_string = date_obs[0:4] + date_obs[5:7] + date_obs[8:10]
        time_string = time_obs[0:2] + time_obs[3:5] + time_obs[6:8]
        filename = 'L' + file_level + '_' + type_code + observatory + '_' + date_string + time_string

        return filename

    def write(self, kind: str = "default", filename: Optional[str] = None) -> Dict:
        """
        Write PUNCHData elements to FITS files

        Parameters
        ----------
        kind
            specified element of the PUNCHData object to write to file
        filename
            output filename (will override default naming scheme)

        Returns
        -------

        """

        # TODO - Note the astropy.fits.Card verification method for headers

        # Generate a standard filename if not overridden
        if filename is None:
            filename = self.generate_filename(kind) + '.fits'

        # Populate elements to write to file
        data = self.cubes[kind].data
        uncert = self.cubes[kind].uncertainty
        meta = self.cubes[kind].meta
        wcs = self.cubes[kind].wcs

        hdu_data = fits.PrimaryHDU()
        hdu_data.data = data
        hdu_data.header = meta
        # Make this something quick until full header writing is enabled

        hdu_uncert = fits.ImageHDU()
        hdu_uncert.data = uncert.array

        hdul = fits.HDUList([hdu_data, hdu_uncert])

        # Write to FITS
        hdul.writeto(filename)

        # TODO - Return dictionary of specified metadata items below (and format these)
        # CREATE TABLE files (
        # file_ id INT UNSIGNED UNIQUE NOT NULL AUTO_INCREMENT,
        # level INT NOT NULL,
        # file_type CHAR(2) NOT NULL,
        # observatory CHAR(1) NOT NULL,
        # file_version INT NOT NULL,
        # software_version INT NOT NULL,
        # date_acquired DATETIME NOT NULL,
        # date_obs DATETIME NOT NULL,
        # date_end DATETIME NOT NULL,
        # polarization CHAR(2),
        # state VARCHAR (64) NOT NULL,
        # processing_fLow CHAR (44) NOT NULL,
        # file_name char (29) GENERATED ALWAYS AS
        # (concat("L", level ," ", file_type, observatory,
        # DATE_FORMAT (date_acquired, 'SY%m%d%H%1%s),
        # , 'V', file_version,
        # ". fits' )),
        # PRIMARY KEY ( file_id ),
        # FOREIGN KEY ( processing_flow )
        # REFERENCES fLows (fLow_id)

        update_table = {}
        return update_table

    def write_image(self, kind: str = "default", filename: Optional[str] = None) -> None:
        """
        Write an 8-bit scaled version of the specified data array to a PNG file

        Parameters
        ----------
        kind
            specified element of the PUNCHData object to write to file
            should be two-dimensional array
        filename
            output filename (will override default naming scheme)

        Returns
        -------

        """

        if self[kind].data.ndim != 2:
            raise Exception("Specified output data should have two-dimensions.")

        # Generate a standard filename if not overridden
        if filename is None:
            filename = self.generate_filename(kind) + '.png'

        # Scale data array to 8-bit values
        output_data = np.fix(np.interp(self[kind].data, (self[kind].data.min(), self[kind].data.max()), (0, 2**8 - 1)))
        output_data = np.int(output_data)

        # Write image to file
        matplotlib.image.saveim(filename, output_data)

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


HistoryEntry = namedtuple("HistoryEntry", "datetime, source, comment")


class History:
    def __init__(self):
        self._entries: List[HistoryEntry] = []

    def add_entry(self, entry: HistoryEntry) -> None:
        self._entries.append(entry)

    def clear(self) -> None:
        self._entries = []

    def __getitem__(self, index: int) -> HistoryEntry:
        return self._entries[index]

    def most_recent(self) -> HistoryEntry:
        return self._entries[-1]

    def convert_to_fits_cards(self) -> None:
        pass

    def __len__(self):
        return len(self._entries)


class PUNCHPlotter(BasePlotter):
    """
    Custom PUNCHPlotter class
    (useful for bespoke colormaps, or special handling of weights / masks)
    """
    def plot(self):
        pass
