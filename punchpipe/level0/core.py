import os
import json
import base64
import warnings
import importlib.metadata
from glob import glob
from typing import Any, Dict, Tuple
from datetime import datetime, timedelta

import astropy.time
import numpy as np
import pylibjpeg
import pymysql
import sqlalchemy.exc
from astropy.wcs import WCS
from prefect import task
from punchbowl.data.wcs import calculate_helio_wcs_from_celestial, calculate_pc_matrix

from punchpipe.control.db import EngLEDPacket, ENGPFWPacket, EngXACTPacket, SciPacket, TLMFiles, get_closest_eng_packets
from punchpipe.control.util import get_database_session
from punchpipe.error import CCSDSPacketConstructionWarning, CCSDSPacketDatabaseUpdateWarning
from punchpipe.level0.ccsds import PACKET_APID2NAME, process_telemetry_file
from punchpipe.level0.meta import determine_file_type, eci_quaternion_to_ra_dec

software_version = importlib.metadata.version("punchpipe")

class PacketEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, bytes):
            return base64.b64encode(obj)
        else:
            return super(PacketEncoder, self).default(obj)


@task
def detect_new_tlm_files(pipeline_config: dict, session=None) -> [str]:
    if session is None:
        session = get_database_session()

    tlm_directory = pipeline_config['tlm_directory']
    found_tlm_files = set(glob(os.path.join(tlm_directory, '**/*.tlm'), recursive=True))
    database_tlm_files = set([p[0] for p in session.query(TLMFiles.path).distinct().all()])
    return sorted(list(found_tlm_files - database_tlm_files))


@task
def parse_new_tlm_files(telemetry_file_path: str) -> dict:
    return process_telemetry_file(telemetry_file_path)


def get_basic_packet_info(packet_name, packets, packet_num):
    try:
        seconds = int(packets[packet_name + "_HDR_SEC"][packet_num])
        microseconds = int(packets[packet_name + "_HDR_USEC"][packet_num])
    except ValueError:
        seconds = 0
        microseconds = 0
        warnings.warn("Time could not be properly extracted for packet.",
                      CCSDSPacketConstructionWarning)
    timestamp = (datetime(2000, 1, 1)
                 + timedelta(seconds=seconds) + timedelta(microseconds=microseconds))

    try:
        spacecraft_id = int(packets[packet_name + "_HDR_SCID"][packet_num])
    except ValueError:
        spacecraft_id = -1
        warnings.warn("Spacecraft ID could not be extracted for packet.",
                      CCSDSPacketConstructionWarning)

    try:
        flash_block_address = int(packets[packet_name + "_HDR_FLASH_BLOCK"][packet_num])
    except ValueError:
        flash_block_address = -1
        warnings.warn("Flash block address could not be extracted for packet.",
                      CCSDSPacketConstructionWarning)

    return timestamp, spacecraft_id, flash_block_address

def form_packet_entry(apid, packets, packet_num, source_tlm_file_id):
    packet_name = PACKET_APID2NAME[apid]

    timestamp, spacecraft_id, flash_block_address = get_basic_packet_info(packet_name, packets, packet_num)

    match packet_name.lower():
        case 'sci_xfi':
            return SciPacket(apid=apid,
                             sequence_count=packets['CCSDS_SEQUENCE_COUNT'][packet_num],
                             length=packets['CCSDS_PACKET_LENGTH'][packet_num],
                             spacecraft_id=spacecraft_id,
                             flash_block=flash_block_address,
                             timestamp=timestamp,
                             packet_num=packet_num,
                             source_tlm_file=source_tlm_file_id,
                             is_used=False,
                             img_pkt_grp=packets['SCI_XFI_HDR_GRP'][packet_num],
                             compression_settings=packets['SCI_XFI_COM_SET'][packet_num],
                             acquisition_settings=packets['SCI_XFI_ACQ_SET'][packet_num], )
        case 'eng_xact':
            return EngXACTPacket(apid=apid,
                                 sequence_count=packets['CCSDS_SEQUENCE_COUNT'][packet_num],
                                 length=packets['CCSDS_PACKET_LENGTH'][packet_num],
                                 spacecraft_id=spacecraft_id,
                                 flash_block=flash_block_address,
                                 timestamp=timestamp,
                                 packet_num=packet_num,
                                 source_tlm_file=source_tlm_file_id,
                                 ATT_DET_Q_BODY_WRT_ECI1=packets['ATT_DET_Q_BODY_WRT_ECI1'][packet_num],
                                 ATT_DET_Q_BODY_WRT_ECI2=packets['ATT_DET_Q_BODY_WRT_ECI2'][packet_num],
                                 ATT_DET_Q_BODY_WRT_ECI3=packets['ATT_DET_Q_BODY_WRT_ECI3'][packet_num],
                                 ATT_DET_Q_BODY_WRT_ECI4=packets['ATT_DET_Q_BODY_WRT_ECI4'][packet_num],
                                 ATT_DET_RESIDUAL1=packets['ATT_DET_RESIDUAL1'][packet_num],
                                 ATT_DET_RESIDUAL2=packets['ATT_DET_RESIDUAL2'][packet_num],
                                 ATT_DET_RESIDUAL3=packets['ATT_DET_RESIDUAL3'][packet_num],
                                 REFS_POSITION_WRT_ECI1=packets['REFS_POSITION_WRT_ECI1'][packet_num],
                                 REFS_POSITION_WRT_ECI2=packets['REFS_POSITION_WRT_ECI2'][packet_num],
                                 REFS_POSITION_WRT_ECI3=packets['REFS_POSITION_WRT_ECI3'][packet_num],
                                 REFS_VELOCITY_WRT_ECI1=packets['REFS_VELOCITY_WRT_ECI1'][packet_num],
                                 REFS_VELOCITY_WRT_ECI2=packets['REFS_VELOCITY_WRT_ECI2'][packet_num],
                                 REFS_VELOCITY_WRT_ECI3=packets['REFS_VELOCITY_WRT_ECI3'][packet_num],
                                 ATT_CMD_CMD_Q_BODY_WRT_ECI1=packets['ATT_CMD_CMD_Q_BODY_WRT_ECI1'][packet_num],
                                 ATT_CMD_CMD_Q_BODY_WRT_ECI2=packets['ATT_CMD_CMD_Q_BODY_WRT_ECI2'][packet_num],
                                 ATT_CMD_CMD_Q_BODY_WRT_ECI3=packets['ATT_CMD_CMD_Q_BODY_WRT_ECI3'][packet_num],
                                 ATT_CMD_CMD_Q_BODY_WRT_ECI4=packets['ATT_CMD_CMD_Q_BODY_WRT_ECI4'][packet_num], )
        case 'eng_pfw':
            return ENGPFWPacket(
                apid=apid,
                sequence_count=packets['CCSDS_SEQUENCE_COUNT'][packet_num],
                length=packets['CCSDS_PACKET_LENGTH'][packet_num],
                spacecraft_id=spacecraft_id,
                flash_block=flash_block_address,
                timestamp=timestamp,
                packet_num=packet_num,
                source_tlm_file=source_tlm_file_id,
                PFW_STATUS=packets['PFW_STATUS'][packet_num],
                STEP_CALC=packets['STEP_CALC'][packet_num],
                LAST_CMD_N_STEPS=packets['LAST_CMD_N_STEPS'][packet_num],
                POSITION_CURR=packets['POSITION_CURR'][packet_num],
                POSITION_CMD=packets['POSITION_CMD'][packet_num],
                RESOLVER_POS_RAW=packets['RESOLVER_POS_RAW'][packet_num],
                RESOLVER_POS_CORR=packets['RESOLVER_POS_CORR'][packet_num],
                RESOLVER_READ_CNT=packets['RESOLVER_READ_CNT'][packet_num],
                LAST_MOVE_N_STEPS=packets['LAST_MOVE_N_STEPS'][packet_num],
                LAST_MOVE_EXECUTION_TIME=packets['LAST_MOVE_EXECUTION_TIME'][packet_num],
                LIFETIME_STEPS_TAKEN=packets['LIFETIME_STEPS_TAKEN'][packet_num],
                LIFETIME_EXECUTION_TIME=packets['LIFETIME_EXECUTION_TIME'][packet_num],
                FSM_CTRL_STATE=packets['FSM_CTRL_STATE'][packet_num],
                READ_SUB_STATE=packets['READ_SUB_STATE'][packet_num],
                MOVE_SUB_STATE=packets['MOVE_SUB_STATE'][packet_num],
                HOME_SUB_STATE=packets['HOME_SUB_STATE'][packet_num],
                HOME_POSITION=packets['HOME_POSITION'][packet_num],
                RESOLVER_SELECT=packets['RESOLVER_SELECT'][packet_num],
                RESOLVER_TOLERANCE_HOME=packets['RESOLVER_TOLERANCE_HOME'][packet_num],
                RESOLVER_TOLERANCE_CURR=packets['RESOLVER_TOLERANCE_CURR'][packet_num],
                STEPPER_SELECT=packets['STEPPER_SELECT'][packet_num],
                STEPPER_RATE_DELAY=packets['STEPPER_RATE_DELAY'][packet_num],
                STEPPER_RATE=packets['STEPPER_RATE'][packet_num],
                SHORT_MOVE_SETTLING_TIME_MS=packets['SHORT_MOVE_SETTLING_TIME_MS'][packet_num],
                LONG_MOVE_SETTLING_TIME_MS=packets['LONG_MOVE_SETTLING_TIME_MS'][packet_num],
                PRIMARY_STEP_OFFSET_1=packets['PRIMARY_STEP_OFFSET_1'][packet_num],
                PRIMARY_STEP_OFFSET_2=packets['PRIMARY_STEP_OFFSET_2'][packet_num],
                PRIMARY_STEP_OFFSET_3=packets['PRIMARY_STEP_OFFSET_3'][packet_num],
                PRIMARY_STEP_OFFSET_4=packets['PRIMARY_STEP_OFFSET_4'][packet_num],
                PRIMARY_STEP_OFFSET_5=packets['PRIMARY_STEP_OFFSET_5'][packet_num],
                REDUNDANT_STEP_OFFSET_1=packets['REDUNDANT_STEP_OFFSET_1'][packet_num],
                REDUNDANT_STEP_OFFSET_2=packets['REDUNDANT_STEP_OFFSET_2'][packet_num],
                REDUNDANT_STEP_OFFSET_3=packets['REDUNDANT_STEP_OFFSET_3'][packet_num],
                REDUNDANT_STEP_OFFSET_4=packets['REDUNDANT_STEP_OFFSET_4'][packet_num],
                REDUNDANT_STEP_OFFSET_5=packets['REDUNDANT_STEP_OFFSET_5'][packet_num],
                PRIMARY_RESOLVER_POSITION_1=packets['PRIMARY_RESOLVER_POSITION_1'][packet_num],
                PRIMARY_RESOLVER_POSITION_2=packets['PRIMARY_RESOLVER_POSITION_2'][packet_num],
                PRIMARY_RESOLVER_POSITION_3=packets['PRIMARY_RESOLVER_POSITION_3'][packet_num],
                PRIMARY_RESOLVER_POSITION_4=packets['PRIMARY_RESOLVER_POSITION_4'][packet_num],
                PRIMARY_RESOLVER_POSITION_5=packets['PRIMARY_RESOLVER_POSITION_5'][packet_num],
                REDUNDANT_RESOLVER_POSITION_1=packets['REDUNDANT_RESOLVER_POSITION_1'][packet_num],
                REDUNDANT_RESOLVER_POSITION_2=packets['REDUNDANT_RESOLVER_POSITION_2'][packet_num],
                REDUNDANT_RESOLVER_POSITION_3=packets['REDUNDANT_RESOLVER_POSITION_3'][packet_num],
                REDUNDANT_RESOLVER_POSITION_4=packets['REDUNDANT_RESOLVER_POSITION_4'][packet_num],
                REDUNDANT_RESOLVER_POSITION_5=packets['REDUNDANT_RESOLVER_POSITION_5'][packet_num],
            )
        case "eng_led":
            return EngLEDPacket(
                apid=apid,
                sequence_count=packets['CCSDS_SEQUENCE_COUNT'][packet_num],
                length=packets['CCSDS_PACKET_LENGTH'][packet_num],
                spacecraft_id=spacecraft_id,
                flash_block=flash_block_address,
                timestamp=timestamp,
                packet_num=packet_num,
                source_tlm_file=source_tlm_file_id,
                LED1_ACTIVE_STATE=packets['LED1_ACTIVE_STATE'][packet_num],
                LED_CFG_NUM_PLS=packets['LED_CFG_NUM_PLS'][packet_num],
                LED2_ACTIVE_STATE=packets['LED2_ACTIVE_STATE'][packet_num],
                LED_CFG_PLS_DLY=packets['LED_CFG_PLS_DLY'][packet_num],
                LED_CFG_PLS_WIDTH=packets['LED_CFG_PLS_WIDTH'][packet_num])
        case _:
            warnings.warn("Unable to add packet to database.", CCSDSPacketDatabaseUpdateWarning)
            return None


@task
def update_tlm_database(packets, tlm_id: int, session=None):
    if session is None:
        session = get_database_session()

    packets_to_save = []
    for apid, this_apid_packets in packets.items():
        if apid in PACKET_APID2NAME:
            step = 10 if apid == 0x69 else 1  # there are so many eng_xact packets so we only take every 10th one
            for i in range(0, len(this_apid_packets['CCSDS_APID']), step):
                try:
                    this_packet = form_packet_entry(apid, this_apid_packets, i, tlm_id)
                    if this_packet is not None:
                        packets_to_save.append(this_packet)
                except (sqlalchemy.exc.DataError, pymysql.err.DataError):
                    raise RuntimeError("FAILED ADDING PACKET")
    session.bulk_save_objects(packets_to_save)
    session.commit()


def interpolate_value(query_time, before_time, before_value, after_time, after_value):
    if query_time == before_time:
        return before_value
    elif query_time == after_time:
        return after_value
    elif before_time == after_time:
        return after_value
    else:
        return ((after_value - before_value)
         * ((query_time - before_time) / (after_time - before_time))
         + before_value)

def get_metadata(observation_time, spacecraft_id, image_shape, session) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    before_xact, after_xact = get_closest_eng_packets(EngXACTPacket, observation_time, spacecraft_id, session)
    ATT_DET_Q_BODY_WRT_ECI1 = interpolate_value(observation_time,
                                                before_xact.timestamp, before_xact.ATT_DET_Q_BODY_WRT_ECI1,
                                                after_xact.timestamp, after_xact.ATT_DET_Q_BODY_WRT_ECI1)
    ATT_DET_Q_BODY_WRT_ECI2 = interpolate_value(observation_time,
                                                before_xact.timestamp, before_xact.ATT_DET_Q_BODY_WRT_ECI2,
                                                after_xact.timestamp, after_xact.ATT_DET_Q_BODY_WRT_ECI2)
    ATT_DET_Q_BODY_WRT_ECI3 = interpolate_value(observation_time,
                                                before_xact.timestamp, before_xact.ATT_DET_Q_BODY_WRT_ECI3,
                                                after_xact.timestamp, after_xact.ATT_DET_Q_BODY_WRT_ECI3)
    ATT_DET_Q_BODY_WRT_ECI4 = interpolate_value(observation_time,
                                                before_xact.timestamp, before_xact.ATT_DET_Q_BODY_WRT_ECI4,
                                                after_xact.timestamp, after_xact.ATT_DET_Q_BODY_WRT_ECI4)

    before_pfw, _ = get_closest_eng_packets(ENGPFWPacket, observation_time, spacecraft_id, session)
    before_led, _ = get_closest_eng_packets(EngLEDPacket, observation_time, spacecraft_id, session)

    position_info = {'spacecraft_id': spacecraft_id,
            'datetime': observation_time,
            'ATT_DET_Q_BODY_WRT_ECI1': ATT_DET_Q_BODY_WRT_ECI1,
            'ATT_DET_Q_BODY_WRT_ECI2': ATT_DET_Q_BODY_WRT_ECI2,
            'ATT_DET_Q_BODY_WRT_ECI3': ATT_DET_Q_BODY_WRT_ECI3,
            'ATT_DET_Q_BODY_WRT_ECI4': ATT_DET_Q_BODY_WRT_ECI4,
            'PFW_POSITION_CURR': before_pfw.POSITION_CURR}

    fits_info = {'TYPECODE': determine_file_type(before_pfw.POSITION_CURR,
                                                 before_led,
                                                 image_shape),
                 'LEDSTATE': before_led.LED_CFG_NUM_PLS}

    return position_info, fits_info


def form_preliminary_wcs(metadata, plate_scale):
    """Create the preliminary WCS for punchbowl"""
    quaternion = np.array([metadata['ATT_DET_Q_BODY_WRT_ECI4'] * 0.5E-10,
                           metadata['ATT_DET_Q_BODY_WRT_ECI1'] * 0.5E-10,
                           metadata['ATT_DET_Q_BODY_WRT_ECI2'] * 0.5E-10,
                           metadata['ATT_DET_Q_BODY_WRT_ECI3'] * 0.5E-10])
    ra, dec, roll = eci_quaternion_to_ra_dec(quaternion)
    projection = "ARC" if metadata['spacecraft_id'] == '4' else 'AZP'
    celestial_wcs = WCS(naxis=2)
    celestial_wcs.wcs.crpix = (1024.5, 1024.5)
    celestial_wcs.wcs.crval = (ra, dec)
    celestial_wcs.wcs.cdelt = plate_scale, plate_scale
    celestial_wcs.wcs.pc = calculate_pc_matrix(roll, celestial_wcs.wcs.cdelt)
    celestial_wcs.wcs.set_pv([(2, 1, 0.0)])  # TODO: makes sure this is reasonably set
    celestial_wcs.wcs.ctype = f"RA--{projection}", f"DEC-{projection}"
    celestial_wcs.wcs.cunit = "deg", "deg"

    return calculate_helio_wcs_from_celestial(celestial_wcs, astropy.time.Time(metadata['datetime']), (2048, 2048))[0]

def image_is_okay(image, pipeline_config):
    """Check that the formed image conforms to image quality expectations"""
    return pipeline_config['quality_check']['mean_low'] < np.mean(image) < pipeline_config['quality_check']['mean_high']

def form_from_jpeg_compressed(packets):
    """Form a JPEG-LS image from packets"""
    img = pylibjpeg.decode(packets.tobytes())
    return img

def form_from_raw(flat_image):
    """Form a raw image from packets"""
    pixel_values = unpack_Nbit_values(flat_image, byteorder=">", N=16)
    nvals = pixel_values.size
    width = 2048
    if nvals % width == 0:
        image = pixel_values.reshape((-1, width))
    else:
        image = np.ravel(pixel_values)[:width * (nvals // width)].reshape((-1, width))
    return image

def unpack_Nbit_values(packed: bytes, byteorder: str, N=19) -> np.ndarray:
    if N in (8, 16, 32, 64):
        trailing = len(packed)%(N//8)
        if trailing:
            packed = packed[:-trailing]
        return np.frombuffer(packed, dtype=np.dtype(f"u{N//8}").newbyteorder(byteorder))
    nbits = len(packed)*8
    bytes_as_ints = np.frombuffer(packed, "u1")
    results = []
    for bit in range(0, nbits, N):
        encompassing_bytes = bytes_as_ints[bit//8:-((bit+N)//-8)]
        # "ceil" equivalent of a//b is -(-a//b), because of
        # http://python-history.blogspot.com/2010/08/why-pythons-integer-division-floors.html
        if len(encompassing_bytes)*8 < N:
           break
        bit_within_byte = bit % 8
        if byteorder in ("little", "<"):
            bytes_value = int.from_bytes(encompassing_bytes, "little")
            bits_value = (bytes_value >> bit_within_byte) & (2**N - 1)
        elif byteorder in ("big", ">"):
            extra_bits_to_right = len(encompassing_bytes)*8 - (bit_within_byte+N)
            bytes_value = int.from_bytes(encompassing_bytes, "big")
            bits_value = (bytes_value >> extra_bits_to_right) & (2**N - 1)
        else:
            raise ValueError("`byteorder` must be either 'little' or 'big'")
        results.append(bits_value)
    return np.asanyarray(results)
