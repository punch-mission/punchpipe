import io
import os

import ccsdspy
import numpy as np
from ccsdspy.utils import split_by_apid

THIS_DIR = os.path.dirname(os.path.abspath(__file__))

PACKET_NAME2APID = {
    "ENG_XACT": 0x69,
    "ENG_CEB": 0x6C,
    "ENG_PFW": 0x6D,
    "ENG_LED": 0x6E,
    "SCI_XFI": 0x20,
}

SKIP_APIDS = [96, 0x64, 0x6B, 0x70, 0x6A, 0x67]

PACKET_APID2NAME = {v: k for k, v in PACKET_NAME2APID.items()}


def open_and_split_packet_file(path: str) -> dict[int, io.BytesIO]:
    with open(path, "rb") as mixed_file:
        stream_by_apid = split_by_apid(mixed_file)
    return stream_by_apid


def load_packet_def(packet_name) -> ccsdspy.VariableLength | ccsdspy.FixedLength:
    if packet_name == "SCI_XFI":
        return ccsdspy.VariableLength.from_file(os.path.join(THIS_DIR, "defs", f"{packet_name}.csv"))
    else:
        return ccsdspy.FixedLength.from_file(os.path.join(THIS_DIR, "defs", f"{packet_name}.csv"))


def process_telemetry_file(telemetry_file_path):
    apid_separated_tlm = open_and_split_packet_file(telemetry_file_path)
    parsed_data = {}
    for apid, stream in apid_separated_tlm.items():
        if apid in PACKET_APID2NAME and apid not in SKIP_APIDS:
            definition = load_packet_def(PACKET_APID2NAME[apid])
            parsed_data[apid] = definition.load(stream, include_primary_header=True)
    return parsed_data


def parse_compression_settings(values):
    # return [{'test': bool(v & 1), 'jpeg': bool(v & 2), 'sqrt': bool(v & 4)} for v in values]
    return [{'test': bool(v & 0b1000000000000000),
             'jpeg': bool(v & 0b0100000000000000),
             'sqrt': bool(v & 0b0010000000000000)} for v in values]


def unpack_compression_settings(com_set_val: "bytes|int"):
    """Unpack image compression control register value.

    See `SciPacket.COMPRESSION_REG` for details."""

    if isinstance(com_set_val, bytes):
        assert len(com_set_val) == 2, f"Compression settings should be a 2-byte field, got {len(com_set_val)} bytes"
        compress_config = int.from_bytes(com_set_val, "big")
    elif isinstance(com_set_val, (int, np.integer)):
        assert com_set_val <= 0xFFFF, f"Compression settings should fit within 2 bytes, got \\x{com_set_val:X}"
        compress_config = int(com_set_val)
    else:
        raise TypeError
    settings_dict = {"SCALE": compress_config >> 8,
                     "RSVD": (compress_config >> 7) & 0b1,
                     "PMB_INIT": (compress_config >> 6) & 0b1,
                     "CMP_BYP": (compress_config >> 5) & 0b1,
                     "BSEL": (compress_config >> 3) & 0b11,
                     "SQRT": (compress_config >> 2) & 0b1,
                     "JPEG": (compress_config >> 1) & 0b1,
                     "TEST": compress_config & 0b1}
    return settings_dict


def unpack_acquisition_settings(acq_set_val: "bytes|int"):
    """Unpack CEB image acquisition register value.

    See `SciPacket.ACQUISITION_REG` for details."""

    if isinstance(acq_set_val, bytes):
        assert len(acq_set_val) == 4, f"Acquisition settings should be a 4-byte field, got {len(acq_set_val)} bytes"
        acquire_config = int.from_bytes(acq_set_val, "big")
    elif isinstance(acq_set_val, (int, np.integer)):
        assert acq_set_val <= 0xFFFFFFFF, f"Acquisition settings should fit within 4 bytes, got \\x{acq_set_val:X}"
        acquire_config = int(acq_set_val)
    else:
        raise TypeError
    settings_dict = {"DELAY": acquire_config >> 24,
                     "IMG_NUM": (acquire_config >> 21) & 0b111,
                     "EXPOSURE": (acquire_config >> 8) & 0x1FFF,
                     "TABLE1": (acquire_config >> 4) & 0b1111,
                     "TABLE2": acquire_config & 0b1111}
    return settings_dict

def get_single_packet(apid_contents: dict[str, np.ndarray], i: int):
    return {k: v[i] for k, v in apid_contents.items()}
