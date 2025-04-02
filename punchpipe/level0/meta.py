import numpy as np
from astropy.coordinates import SkyCoord
from prefect import task

PFW_POSITIONS = {"M": 960,
                 "opaque": 720,
                 "Z": 480,
                 "P": 240,
                 "Clear": 0}

POSITIONS_TO_CODES = {"Clear": "CR", "P": "PP", "M": "PM", "Z": "PZ"}

PFW_POSITION_MAPPING = ["Manual", "M", "opaque", "Z", "P", "Clear"]

def convert_pfw_position_to_polarizer(pfw_position):
    differences = {key: abs(pfw_position - reference_position) for key, reference_position in PFW_POSITIONS.items()}
    return min(differences, key=differences.get)


@task
def determine_file_type(polarizer_position, led_info, image_shape) -> str:
    if led_info is not None:
        return "DY"
    elif image_shape != (2048, 2048):
        return "OV"
    elif polarizer_position == 0 or polarizer_position == 2:  # TODO: position = 0 is manual pointing... it shouldn't be DK
        return "DK"
    else:
        return POSITIONS_TO_CODES[PFW_POSITION_MAPPING[polarizer_position]]


def eci_quaternion_to_ra_dec(q):
    """
    Convert an ECI quaternion to RA and Dec.

    Args:
        q: A numpy array representing the ECI quaternion (q0, q1, q2, q3).

    Returns:
        ra: Right Ascension in degrees.
        dec: Declination in degrees.
    """

    # Normalize the quaternion
    q = q / np.linalg.norm(q)

    w, x, y, z = q
    # Calculate the rotation matrix from the quaternion
    R = np.array([[1 - 2*(y**2 + z**2), 2*(x*y - z*w), 2*(x*z + y*w)],
         [2*(x*y + z*w), 1 - 2*(x**2 + z**2), 2*(y*z - x*w)],
         [2*(x*z - y*w), 2*(y*z + x*w), 1 - 2*(x**2 + y**2)]])

    axis_eci = np.array([1, 0, 0])
    body = R @ axis_eci

    # Calculate RA and Dec from the rotated z-vector
    c = SkyCoord(body[0], body[1], body[2], representation_type='cartesian', unit='m').fk5
    ra = c.ra.deg
    dec = c.dec.deg
    roll = np.arctan2(q[1] * q[2] - q[0] * q[3], 1 / 2 - (q[2] ** 2 + q[3] ** 2))

    return ra, dec, roll
