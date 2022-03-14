import numpy as np
from punchpipe.infrastructure.tasks.core import ScienceFunction
from punchpipe.infrastructure.data import PUNCHData
from punchpipe.infrastructure.tasks.core import CalibrationConfiguration


class DestreakFunction(ScienceFunction):
    @staticmethod
    def streak_correction_matrix(n: int, diag: float, below: float, above: float) -> np.ndarray:
        """Computes a matrix used in correcting streaks in PUNCH images

        Computes the inverse of a matrix of size n where the major diagonal
            contains the value diag, the lower triangle contains below and the
            upper triangle contains the value above.
                i.e. X[i,i]=diag, X[0:i-1,i]=below, X[0,i+1:n-1]=above

        Adapted from solarsoft sc_inverse

        Parameters
        ----------
        n
            size of the matrix (n x n)
        diag
            value on the diagonal of the matrix, i.e. the exposure time
        below
            value in the lower triangle, i.e. the readout line time
        above
            value in the upper triangle, i.e. the reset line time

        Returns
        -------
        np.ndarray
            value of specified streak correction array

        # TODO : add example call
        """
        L = np.tril(np.ones((n, n)) * below, -1)
        U = np.triu(np.ones((n, n)) * above, 1)
        D = np.diagflat(np.ones(n) * diag)
        M = L + U + D
        return np.linalg.inv(M)

    @staticmethod
    def correct_streaks(image: np.ndarray,
                        exposure_time: float,
                        readout_line_time: float,
                        reset_line_time: float) -> np.ndarray:
        """Corrects an image for streaks

        Parameters
        ----------
        image
            image to be corrected
        exposure_time
            exposure time in consistent units (e.g. milliseconds) with readout_line_time and reset_line time
        readout_line_time
            time to readout a line in consistent units (e.g. milliseconds) with exposure_time and reset_line time
        reset_line_time
            time to reset CCD in consistent units (e.g. milliseconds) with readout_line_time and exposure_time

        Returns
        -------
        np.ndarray
            a streak-corrected image

        # TODO: add example call
        """
        assert len(image.shape) == 2, "must be a 2-D image"
        assert np.equal(*image.shape), "must be a square image"
        correction_matrix = DestreakFunction.streak_correction_matrix(image.shape[0],
                                                                      exposure_time,
                                                                      readout_line_time,
                                                                      reset_line_time)
        return correction_matrix @ image

    def process(self, data_object: PUNCHData, parameters: CalibrationConfiguration) -> PUNCHData:
        # do the stuff on the actual data object
        # 1. get the data out somehow from PUNCHDataObject
        # 2. run the static methods on it
        # 3. put it back in the data object
        # 4. track history
        pass
