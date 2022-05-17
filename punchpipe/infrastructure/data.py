from __future__ import annotations

from collections import namedtuple
from datetime import datetime
from typing import Union, Optional, List, Dict
import astropy.units as u
import matplotlib
from typing import Union, Optional, List, Dict
import numpy as np
from astropy.io import fits
from astropy.nddata import StdDevUncertainty
from astropy.wcs import WCS
from dateutil.parser import parse as parse_datetime
from ndcube import NDCube
from ndcube.visualization import BasePlotter

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

    def __len__(self) -> int:
        return len(self._entries)

    def __str__(self) -> str:
        return "\n".join([f"{e.datetime}: {e.source}: {e.comment}" for e in self._entries])


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

    def __init__(self, data_obj: Union[dict, NDCube, None]):
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
            self._cubes = data_obj
        elif isinstance(data_obj, NDCube):
            self._cubes = ("default", data_obj)
        elif data_obj is None:
            self._cubes = dict()
        else:
            raise Exception("Please specify either an NDCube object, or a dictionary of NDCube objects")

        self._history = History()

    def add_history(self, time: datetime, source: str, comment: str):
        self._history.add_entry(HistoryEntry(time, source, comment))

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
                if type(file) is str:
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

        return 1./self._cubes[kind].uncertainty.array

    def __contains__(self, kind) -> bool:
        return kind in self._cubes

    def __getitem__(self, kind) -> NDCube:
        return self._cubes[kind]

    def __setitem__(self, kind, data) -> None:
        if type(data) is NDCube:
            self._cubes[kind] = data
        else:
            raise Exception("PUNCHData entries must contain NDCube objects.")

    def __delitem__(self, kind) -> None:
        del self._cubes[kind]

    def clear(self) -> None:
        """remove all NDCubes"""
        self._cubes.clear()

    def update(self, other: PUNCHData) -> None:
        """merge two PUNCHData objects"""
        self._cubes.update(other)

    def generate_id(self, kind: str = "default") -> str:
        """
        Dynamically generate an identification string for the given data product, using the format 'Ln_ttO_yyyymmddhhmmss'
        Parameters
        ----------
        kind
            specified element of the PUNCHData object to write to file

        Returns
        -------
        id
            output identification string

        """
        observatory = self.get_meta(key='OBSRVTRY', kind=kind)
        file_level = self.get_meta(key='LEVEL', kind=kind)
        type_code = self.get_meta(key='TYPECODE', kind=kind)
        date_obs = self._cubes[kind].date_obs
        date_string = date_obs.strftime("%Y%m%d%H%M%S")

        filename = 'L' + file_level + '_' + type_code + observatory + '_' + date_string

        return filename

    def write(self, filename: str, kind: str = "default", overwrite=True) -> Dict:
        """
        Write PUNCHData elements to file

        Parameters
        ----------
        filename
            output filename (including path and file extension)
        kind
            specified element of the PUNCHData object to write to file
        overwrite
            True will overwrite an exsiting file, False will create an execption if a file exists

        Returns
        -------
        update_table
            dictionary of pipeline metadata

        """

        if filename.endswith('.fits'):
            self._write_fits(filename, kind, overwrite=overwrite)
        elif filename.endswith('.png'):
            self._write_ql(filename, kind)
        elif (filename.endswith('.jpg')) or (filename.endswith('.jpeg')):
            self._write_ql(filename, kind)
        else:
            raise Exception('Please specify a valid file extension (.fits, .png, .jpg, .jpeg)')

        update_table = {}
        update_table['file_id'] = filename
        update_table['level'] = self.get_meta(key='LEVEL', kind=kind)
        update_table['file_type'] = self.get_meta(key='TYPECODE', kind=kind)
        update_table['observatory'] = self.get_meta(key='OBSRVTRY', kind=kind)
        update_table['file_version'] = self.get_meta(key='VERSION', kind=kind)
        update_table['software_version'] = self.get_meta(key='SOFTVERS', kind=kind)
        update_table['date_acquired'] = self.get_meta(key='DATE-AQD', kind=kind)
        update_table['date_obs'] = self.get_meta(key='DATE-OBS', kind=kind)
        update_table['date_end'] = self.get_meta(key='DATE-END', kind=kind)
        update_table['polarization'] = self.get_meta(key='POL', kind=kind)
        update_table['state'] = self.get_meta(key='STATE', kind=kind)
        update_table['processing_flow'] = self.get_meta(key='PROCFLOW', kind=kind)
        update_table['file_name'] = filename

        return update_table

    def _write_fits(self, filename: str, kind: str = "default", overwrite=True) -> None:
        """
        Write PUNCHData elements to FITS files

        Parameters
        ----------
        filename
            output filename (including path and file extension)
        kind
            specified element of the PUNCHData object to write to file
        overwrite
            True will overwrite an exsiting file, False will throw an exeception in that scenario

        Returns
        -------
        None

        """

        # Populate elements to write to file
        data = self._cubes[kind].data
        uncert = self._cubes[kind].uncertainty
        meta = self._cubes[kind].meta
        wcs = self._cubes[kind].wcs

        hdu_data = fits.PrimaryHDU()
        hdu_data.data = data
        for key, value in meta.items():
            hdu_data.header[key] = value
        for entry in self._history._entries:
            hdu_data.header['HISTORY'] = f"{entry.datetime}: {entry.source}, {entry.comment}"


        hdu_uncert = fits.ImageHDU()
        hdu_uncert.data = uncert.array

        hdul = fits.HDUList([hdu_data, hdu_uncert])

        # Write to FITS
        hdul.writeto(filename, overwrite=overwrite)

    def _write_ql(self, filename: str, kind: str = "default") -> None:
        """
        Write an 8-bit scaled version of the specified data array to a PNG file

        Parameters
        ----------
        filename
            output filename (including path and file extension)
        kind
            specified element of the PUNCHData object to write to file

        Returns
        -------
        None

        """

        if self[kind].data.ndim != 2:
            raise Exception("Specified output data should have two-dimensions.")

        # Scale data array to 8-bit values
        output_data = np.int(np.fix(np.interp(self[kind].data, (self[kind].data.min(), self[kind].data.max()), (0, 2**8 - 1))))

        # Write image to file
        matplotlib.image.saveim(filename, output_data)

    def plot(self, kind: str = "default") -> None:
        """Generate relevant plots to display or file"""
        self._cubes[kind].show()

    def get_meta(self, key: str, kind: str = "default") -> Union[str, int, float]:
        """
        Retrieves metadata about a cube
        Parameters
        ----------
        key
            specified metadata key
        kind
            specified element of the PUNCHData object to write to file

        Returns
        -------
        Requested metadata

        """
        return self._cubes[kind].meta[key]

    def set_meta(self, key: str, value: str, kind: str = "default") -> None:
        """
        Retrieves metadata about a cube
        Parameters
        ----------
        key
            specified metadata key
        value
            Updated metadata information
        kind
            specified element of the PUNCHData object to write to file

        Returns
        -------
        None

        """
        self._cubes[kind].meta[key] = value

    def date_obs(self, kind: str = "default") -> datetime:
        return parse_datetime(self.get_meta(key='DATE-OBS', kind=kind))
