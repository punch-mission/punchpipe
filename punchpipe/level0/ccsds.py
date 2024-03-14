import io

from ccsdspy import (
    PacketArray,
    PacketField,
    VariableLength,
    FixedLength
)
from ccsdspy.utils import split_by_apid
import pandas as pd


def open_and_split_packet_file(path: str) -> dict[int, io.BytesIO]:
    with open(path, 'rb') as mixed_file:
        stream_by_apid = split_by_apid(mixed_file)
    return stream_by_apid


def load_packet_def(packet_name, definition_path: str = 'packets/2024-02-09/PUNCH_TLM.xls'):
    if packet_name == "SCI_XFI":
        return _load_science_packet_def(packet_name, definition_path)
    else:
        return _load_engineering_packet_def(packet_name, definition_path)


def _load_engineering_packet_def(packet_name, definition_path="packets/2024-02-09/PUNCH_TLM.xls"):
    contents = pd.read_excel(definition_path, sheet_name=packet_name)

    definition = []
    for row in contents.iterrows():
        name = row[1].iloc[0]
        kind = row[1].iloc[2]
        kind = 'uint' if name not in("FILL_VALUE", "FSW_MEM_DUMP_DATA") else "fill"
        start_byte = row[1].iloc[6]
        start_bit = row[1].iloc[7]
        size = row[1].iloc[8]
        definition.append(PacketField(name=name, data_type=kind, bit_length=size))
    return FixedLength(definition)


def _load_science_packet_def(packet_name, definition_path="packets/2024-02-09/PUNCH_TLM.xls"):
    sci_pkt = VariableLength([
        PacketField(
            name='SCI_XFI_HDR_SCID',
            data_type='uint',
            bit_length=8
        ),
        PacketField(
            name='SCI_XFI_FILL_1',
            data_type='fill',
            bit_length=1
        ),
        PacketField(
            name='SCI_XFI_FLASH_ADDR',
            data_type='uint',
            bit_length=15
        ),
        PacketField(
            name='SCI_XFI_FILL_2',
            data_type='fill',
            bit_length=2,
        ),
        PacketField(
            name='SCI_XFI_TIME_QUAL',
            data_type='uint',
            bit_length=2
        ),
        PacketField(
            name='SCI_XFI_GPS_TIME_MS',
            data_type='uint',
            bit_length=20,
        ),
        PacketField(
            name='SCI_XFI_GPS_TIME_S',
            data_type='uint',
            bit_length=32,
        ),
        PacketField(
            name='SCI_XFI_HDR_GRP',
            data_type='uint',
            bit_length=8,
        ),
        PacketField(
            name='SCI_XFI_ACQ_SET',
            data_type='uint',
            bit_length=32,
        ),
        PacketField(
            name='SCI_XFI_COM_SET',
            data_type='uint',
            bit_length=16,
        ),
        PacketField(
            name='SCI_XFI_FILL_3',
            data_type='fill',
            bit_length=8,
        ),
        PacketArray(
            name='SCI_XFI_IMG_DATA',
            data_type='uint',
            bit_length=8,
            array_shape='expand'
        )
    ])

    return sci_pkt


def process_telemetry_file(telemetry_file_path):
    apid_separated_tlm = open_and_split_packet_file(telemetry_file_path)
