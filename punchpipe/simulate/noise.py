import numpy as np
#from punchpipe.infrastructure.data import PUNCHData


class Noise:
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
        """Generates noise based on an input data array, with specified noise parameters

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
        data = np.interp(data, (data.min(), data.max()), (0, 2 ** bitrate_signal - 1))
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
        data = data + noise_sum

        return noise_sum

    @staticmethod
    def compress(data: np.ndarray = np.zeros([2048,2048]),
                 bitrate: int = 12) -> np.ndarray:
        """

        Parameters
        ----------
        bitrate

        Returns
        -------

        """
        # Or... just create a lookup table to go directly to the 12-bit
        # Should be all zero up to the bias, linear up to a good range, and then scale like the square root
        # Should always map to the middle or average of the block

        # Remember that the photon noise goes as the square root, so at a certain point
        # each pixel has this kind of uncertainty anyway.
        # The goal is to square root encode to lose that information anyway, condensing
        # a range of values to a single value of that width: | > > o < < | > > o < < | > > o < < |

        # 16 bit array - scale to 24 - take square root - round it : simple mapping 16->12
        # Define the reverse lookup

        data_16 = (np.copy(dat2)).astype('long')

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
