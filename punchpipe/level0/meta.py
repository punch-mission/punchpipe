import numpy as np
from astropy.coordinates import SkyCoord

PFW_POSITIONS = {"M": 960,
                 "opaque": 720,
                 "Z": 480,
                 "P": 240,
                 "Clear": 0}

POSITIONS_TO_CODES = {"Clear": "CR", "P": "PP", "M": "PM", "Z": "PZ"}

def convert_pfw_position_to_polarizer(pfw_position):
    differences = {key: abs(pfw_position - reference_position) for key, reference_position in PFW_POSITIONS.items()}
    return min(differences, key=differences.get)


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

    # Calculate the rotation matrix from the quaternion
    R = np.array([
        [1 - 2 * (q[2]**2 + q[3]**2), 2 * (q[1] * q[2] - q[0] * q[3]), 2 * (q[1] * q[3] + q[0] * q[2])],
        [2 * (q[1] * q[2] + q[0] * q[3]), 1 - 2 * (q[1]**2 + q[3]**2), 2 * (q[2] * q[3] - q[0] * q[1])],
        [2 * (q[1] * q[3] - q[0] * q[2]), 2 * (q[2] * q[3] + q[0] * q[1]), 1 - 2 * (q[1]**2 + q[2]**2)]
    ])

    # Extract the unit vector pointing in the z-direction (ECI frame)
    z_eci = np.array([0, 0, 1])

    # Rotate the z-vector to the body frame
    z_body = R @ z_eci

    # Calculate RA and Dec from the rotated z-vector
    c = SkyCoord(z_body[0], z_body[1], z_body[2], representation_type='cartesian', unit='m').fk5
    ra = c.ra.deg
    dec = c.dec.deg
    roll = np.arctan2(q[1] * q[2] - q[0] * q[3], 1 / 2 - (q[2] ** 2 + q[3] ** 2))

    return ra, dec, roll
