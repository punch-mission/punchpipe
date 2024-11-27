import io
import os

import ccsdspy
import numpy as np
import pylibjpeg
from ccsdspy.utils import split_by_apid
from matplotlib import pyplot as plt

THIS_DIR = os.path.dirname(os.path.abspath(__file__))

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
        print(apid)
        if apid not in PACKET_APID2NAME or apid in SKIP_APIDS:
            print(f"skipping {apid}")
        else:
            print(apid, PACKET_APID2NAME[apid])
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


# if __name__ == "__main__":
#     from punchbowl.data.visualize import cmap_punch
#
#     path = "/Users/jhughes/new_results/nov17-0753/PUNCH_EM-01_RAW_2024_320_22_36_V01.tlm"
#     # path = "/Users/jhughes/Desktop/data/PUNCH_CCSDS/RAW_CCSDS_DATA/PUNCH_WFI01_RAW_2024_117_22_00_V01.tlm"
#     parsed = process_telemetry_file(path)
#     print(parsed[0x20].keys())
#     for i in range(len(parsed[0x20])):
#         print(i)
#         print(unpack_compression_settings(parsed[0x20]['SCI_XFI_COM_SET'][i]))
#         print(unpack_acquisition_settings(parsed[0x20]['SCI_XFI_COM_SET'][i]))
#         print("-"*80)
#     #
#     # fig, ax = plt.subplots()
#     # ax.plot(parsed[0x20]['SCI_XFI_HDR_SEC'])
#     # plt.show()
#
#     print({k: len(parsed[k]) for k in parsed})
#
#     print(parsed[0x20]['CCSDS_PACKET_LENGTH'])
#     print(parsed[0x20]['SCI_XFI_HDR_SCID'])
#
#     img = np.concatenate(parsed[0x20]['SCI_XFI_IMG_DATA'][5:24])
#     # img = parsed[0x20]['SCI_XFI_IMG_DATA'][0]
#     img = pylibjpeg.decode(img.tobytes())
#
#     from punchbowl.data.io import load_ndcube_from_fits
#     cube = load_ndcube_from_fits("/Users/jhughes/new_results/nov17-0753/PUNCH_L0_PZ2_20241002142916_v1.fits")
#
#     # vmin, vmax = 0, 1_000
    # fig, axs = plt.subplots(ncols=2, sharex=True, sharey=True)
    # im0 = axs[0].imshow(img, vmin=np.sqrt(vmin*8), vmax=np.sqrt(8*vmax), interpolation="none")
    # im1 = axs[1].imshow(cube.data, vmin=vmin, vmax=vmax, interpolation="none")
    # axs[0].set_title("Unpacked image")
    # axs[1].set_title("SOC image")
    # fig.colorbar(im0, ax=axs[0])
    # fig.colorbar(im1, ax=axs[1])
    # plt.show()

    # fig, ax = plt.subplots()
    # ax.plot(cube.data.flatten(), img.flatten(), 'bo', label='data')
    # ax.plot(np.arange(65_000), np.sqrt(np.arange(65_000)*8).astype(int), 'k', label='sqrt(8x)')
    # ax.set_xlabel("SOC image value")
    # ax.set_ylabel("Unpacked image value")
    # ax.set_xlim((50_000, 70_000))
    # ax.set_ylim((np.sqrt(8*50_000), np.sqrt(8*70_000)))
    #
    # ax.legend()
    # plt.show()
    #
    # vmin, vmax = 0, 1_600
    # fig, axs = plt.subplots(ncols=2, sharex=True, sharey=True, figsize=(12, 6))
    # im0 = axs[1].imshow(img, vmin=np.sqrt(vmin * 8), vmax=np.sqrt(8 * vmax), interpolation="none", cmap=cmap_punch())
    # im1 = axs[0].imshow(np.sqrt(cube.data*8), vmin=np.sqrt(vmin * 8), vmax=np.sqrt(8 * vmax), interpolation="none", cmap=cmap_punch())
    # axs[1].set_title("Output test image")
    # axs[0].set_title("Input test image")
    # # fig.colorbar(im0, ax=axs[0])
    # # fig.colorbar(im1, ax=axs[1])
    # fig.tight_layout()
    # fig.savefig("mmr_image.png", dpi=300)
    # plt.show()

if __name__ == "__main__":
    from punchpipe.level0.core import update_tlm_database
    from punchpipe.flows.level0 import level0_form_images, level0_ingest_raw_packets
    # path = "/Users/jhughes/dropzone/PUNCH_WFI01_RAW_2024_325_16_23_V01.tlm"
    # level0_ingest_raw_packets()
    # packets = process_telemetry_file(path)
    # print(packets)
    # update_tlm_database(packets, 0)

    level0_form_images()