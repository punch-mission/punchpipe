import numpy as np
# from punchpipe.infrastructure.data import PUNCHData
from punchpipe.infrastructure.tasks.core import ScienceFunction


class Noise(ScienceFunction):
    """
    Class for instrument noise simulation
    """

    @staticmethod
    def gen_noise(data: np.ndarray = np.zeros([2048, 2048]),
                  bias_level: float = 100,
                  dark_level: float = 55.81,
                  gain: float = 4.3,
                  readnoise_level: float = 17,
                  bitrate_signal: int = 16) -> np.ndarray:
        """
        Generates noise based on an input data array, with specified noise parameters

        Parameters
        ----------
        data
            input data array (n x n)
        bias_level
            ccd bias level
        dark_level
            ccd dark level
        gain
            ccd gain
        readnoise_level
            ccd read noise level
        bitrate_signal
            desired ccd data bit level

        Returns
        -------
        np.ndarray
            computed noise array corresponding to input data and ccd/noise parameters

        """

        # Generate a copy of the input signal
        data_signal = np.copy(data)

        # Convert / scale data
        # Think of this as the raw signal input into the camera
        data = np.interp(data, (data.min, data.max), (0, 2 ** bitrate_signal - 1))
        data = data.astype('long')

        # Add bias level and clip pixels to avoid overflow
        data = np.clip(data + bias_level, 0, 2 ** bitrate_signal - 1)

        # Photon / shot noise generation
        data_photon = data_signal * gain  # DN to photoelectrons
        sigma_photon = np.sqrt(data_photon)  # Converting sigma of this
        sigma = sigma_photon / gain  # Converting back to DN
        noise_photon = np.random.normal(scale=sigma)

        # Dark noise generation
        noise_level = dark_level * gain
        noise_dark = np.random.poisson(lam=noise_level, size=data.shape) / gain

        # Read noise generation
        noise_read = np.random.normal(scale=readnoise_level, size=data.shape)
        noise_read = noise_read / gain  # Convert back to DN

        # Add these noise terms in quadrature
        noise_quad = np.sqrt(noise_photon ** 2 + noise_dark ** 2 + noise_read ** 2)

        # And then add noise terms directly
        noise_sum = noise_photon + noise_dark + noise_read

        return noise_sum

    @staticmethod
    def encode(data: np.ndarray = np.zeros([2048,2048]),
               frombits: int = 16,
               tobits: int = 12) -> np.ndarray:



        lookup_table = np.sqrt(np.interp(np.arange(0, 2 ** frombits, 1), (0, 2 ** frombits - 1), (0, 2 ** (frombits * 2) - 1)))
        lookup_16_12 = np.round(lookup_16_12).astype(np.uint16)
        lookup_16_12[np.where(lookup_16_12 == 4096)] = 4095

        data_encoded =

        return data_encoded

    @staticmethod
    def decode(data: np.ndarray = np.zeros([2048,2048]),
                 bitrate: int = 12) -> np.ndarray:
        """

        Parameters
        ----------
        bitrate

        Returns
        -------
        np.ndarray
            square root encoded version of the input image

        """

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

        ccd_gain = 1.0 / 4.3  # DN/electron
        ccd_offset = 100  # DN
        ccd_fixedsigma = 17  # DN
        ccd_bits = 16
        im_bits = ccd_bits
        sq_bits = 10

        tab = gen_decode_table(ccd_bits, sq_bits, ccd_gain, ccd_offset, ccd_fixedsigma)

        data_16 = (np.copy(data)).astype('long')

        # Define a lookup table from 16-bit to 12-bit (and 11-bit, 10-bit)
        lookup_16_12 = np.sqrt(np.interp(np.arange(0, 2 ** 16, 1), (0, 2 ** 16 - 1), (0, 2 ** 24 - 1)))
        lookup_16_12 = np.round(lookup_16_12).astype(np.uint16)
        lookup_16_12[np.where(lookup_16_12 == 4096)] = 4095

        lookup_16_11 = np.sqrt(np.interp(np.arange(0, 2 ** 16, 1), (0, 2 ** 16 - 1), (0, 2 ** 24 - 1)) / 4)
        lookup_16_11 = np.round(lookup_16_11).astype(np.uint16)
        lookup_16_11[np.where(lookup_16_11 == 2048)] = 2047

        lookup_16_10 = np.sqrt(np.interp(np.arange(0, 2 ** 16, 1), (0, 2 ** 16 - 1), (0, 2 ** 24 - 1)) / 16)
        lookup_16_10 = np.round(lookup_16_10).astype(np.uint16)
        lookup_16_10[np.where(lookup_16_10 == 1024)] = 1023

        return data_decoded

# TODO - Add prefect task stuff here.