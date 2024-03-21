import io
import os

import ccsdspy
from ccsdspy.utils import split_by_apid

PACKET_NAME2APID = {
    "ENG_LZ": 0x60,
    "ENG_BOOT": 0x61,
    "ENG_EVT": 0x62,
    "ENG_DNLD": 0x63,
    "ENG_FDC": 0x64,
    "ENG_SEQ": 0x65,
    "ENG_HI": 0x66,
    "ENG_LO": 0x67,
    "ENG_SSV": 0x68,
    "ENG_XACT": 0x69,
    "ENG_HYD": 0x6A,
    "ENG_XTS": 0x6B,
    "ENG_CEB": 0x6C,
    "ENG_PFW": 0x6D,
    "ENG_LED": 0x6E,
    "ENG_STM_ECHO": 0x75,
    "ENG_STM_HK": 0x76,
    "ENG_STM_DUMP": 0x77,
    "ENG_STM_LOG": 0x78,
    "ENG_STM_DIAG": 0x79,
    "SCI_XFI": 0x20,
    "ENG_COMSEC": 0x70,
    "ENG_FILL": 0x71,
}

PACKET_APID2NAME = {v: k for k, v in PACKET_NAME2APID.items()}


def open_and_split_packet_file(path: str) -> dict[int, io.BytesIO]:
    with open(path, "rb") as mixed_file:
        stream_by_apid = split_by_apid(mixed_file)
    return stream_by_apid


def load_packet_def(packet_name) -> ccsdspy.VariableLength | ccsdspy.FixedLength:
    if packet_name == "SCI_XFI":
        return ccsdspy.VariableLength.from_file(os.path.join("./defs", f"{packet_name}.csv"))
    else:
        return ccsdspy.FixedLength.from_file(os.path.join("./defs", f"{packet_name}.csv"))


def process_telemetry_file(telemetry_file_path):
    apid_separated_tlm = open_and_split_packet_file(telemetry_file_path)
    parsed_data = {}
    for apid, stream in apid_separated_tlm.items():
        definition = load_packet_def(PACKET_APID2NAME[apid])
        parsed_data[apid] = definition.load(stream, include_primary_header=True)
    return parsed_data


if __name__ == "__main__":
    path = "/Users/jhughes/Desktop/sdf/punchbowl/Level0/packets/2024-02-09/PUNCH_NFI00_RAW_2024_040_21_32_V01.tlm"
    parsed = process_telemetry_file(path)
    print(parsed[0x20])
