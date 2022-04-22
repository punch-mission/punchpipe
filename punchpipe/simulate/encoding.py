import numpy as np
from punchpipe.infrastructure.data import PUNCHData
from punchpipe.infrastructure.tasks.core import ScienceFunction


class Encoding(ScienceFunction):

    @staticmethod
    def encode(data: np.ndarray = np.zeros([2048, 2048]),
               frombits: int = 16,
               tobits: int = 12) -> np.ndarray:
        """
        Square root encode between specified bitrate values

        Parameters
        ----------
        data
            Input data array (n x n)
        frombits
            Specified bitrate of original input image
        tobits
            Specifed bitrate of output encoded image

        Returns
        -------
        np.ndarray
            Encoded version of input data (n x n)

        """

        data = np.round(data).astype(np.ulonglong).clip(0, None)
        ibits = tobits * 2
        factor = np.array(2 ** (ibits - frombits)).astype(np.ulonglong)
        s2 = (data * factor).astype(np.ulonglong)
        return np.round(np.sqrt(s2)).astype(np.ulonglong)

    @staticmethod
    def decode(data: np.ndarray = np.zeros([2048, 2048]),
               bias_level: float = 100,
               gain: float = 4.3,
               readnoise_level: float = 17,
               frombits: int = 12,
               tobits: int = 16) -> np.ndarray:
        """
        Square root decode between specified bitrate values

        Parameters
        ----------
        data
            input encoded data array (n x n)
        bias_level
            ccd bias level
        gain
            ccd gain
        readnoise_level
            ccd read noise level
        frombits
            Specified bitrate of encoded image to unpack
        tobits
            Specified bitrate of output data (decoded)

        Returns
        -------
        np.ndarray
            square root decoded version of the input image (n x n)

        """

        # Define some functions to be used internally

        def encode(source, frombits, tobits):
            source = np.round(source).astype(np.ulonglong).clip(0, None)
            ibits = tobits * 2
            factor = np.array(2 ** (ibits - frombits)).astype(np.ulonglong)
            s2 = (source * factor).astype(np.ulonglong)
            return np.round(np.sqrt(s2)).astype(np.ulonglong)

        def decode(source, frombits, tobits):
            source = np.round(source).astype(np.ulonglong).clip(0, None)
            ibits = tobits * 2
            factor = 2 ** (ibits - frombits)
            s2 = source * source
            return np.round(s2 / factor).astype(np.ulonglong)

        def decode_floating(source, frombits, tobits):
            s1 = np.floor(source)
            s2 = np.ceil(source)
            alpha = s2 - source
            return alpha * (decode(s1, frombits, tobits)) + (1 - alpha) * (decode(s2, frombits, tobits))

        def noise_sigma(val, ccd_gain, ccd_offset, ccd_fixedsigma, n_sigma=3, n_steps=10001):
            electrons = np.clip((val - ccd_offset) / ccd_gain, 1, None)
            poisson_sigma = np.sqrt(electrons) * ccd_gain
            sigma = np.sqrt(poisson_sigma ** 2 + ccd_fixedsigma ** 2)
            step = sigma * n_sigma * 2 / (n_steps + 1)
            dn_steps = np.arange(-n_sigma * sigma, val + n_sigma * sigma + 0.5 * step, step)
            normal = np.exp(- dn_steps * dn_steps / sigma / sigma / 2)
            normal = normal / np.sum(normal)
            return (val + dn_steps, normal)

        def mean_offset(sval, frombits, tobits, ccd_gain, ccd_offset, ccd_fixedsigma):
            val = decode(sval, frombits, tobits)
            (vals, weights) = noise_sigma(val, ccd_gain, ccd_offset, ccd_fixedsigma)
            svals = encode(vals, frombits, tobits)
            ev = np.sum(svals * weights)
            return ev - sval

        def decode_corrected(sval, frombits, tobits, ccd_gain, ccd_offset, ccd_fixedsigma):
            of = mean_offset(sval, frombits, tobits, ccd_gain, ccd_offset, ccd_fixedsigma)
            return decode_floating(sval - of, frombits, tobits)

        def gen_decode_table(frombits, tobits, ccd_gain, ccd_offset, ccd_fixedsigma):
            output = np.zeros(2 ** tobits)
            for i in range(0, 2 ** tobits):
                output[i] = decode_corrected(i, frombits, tobits, ccd_gain, ccd_offset, ccd_fixedsigma)
            return output

        def decode_bytable(s, table):
            s = np.round(s).clip(0, table.shape[0] - 1).astype(np.ulonglong)
            return table[s]

        tab = gen_decode_table(tobits, frombits, gain, bias_level, readnoise_level)

        return decode_bytable(data, tab)
