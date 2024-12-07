import os
import json
import base64
import warnings
import importlib.metadata
from glob import glob
from datetime import datetime, timedelta

import astropy.time
import numpy as np
import pylibjpeg
import pymysql
import sqlalchemy.exc
from astropy.wcs import WCS
from prefect import task
from punchbowl.data.wcs import calculate_helio_wcs_from_celestial, calculate_pc_matrix
from sunpy.coordinates import sun

from punchpipe.control.db import ENGPFWPacket, EngXACTPacket, SciPacket, TLMFiles, get_closest_eng_packets
from punchpipe.control.util import get_database_session
from punchpipe.error import CCSDSPacketConstructionWarning, CCSDSPacketDatabaseUpdateWarning
from punchpipe.level0.ccsds import PACKET_APID2NAME, get_single_packet, process_telemetry_file
from punchpipe.level0.meta import eci_quaternion_to_ra_dec

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
    return list(found_tlm_files - database_tlm_files)


@task
def parse_new_tlm_files(telemetry_file_path: str) -> dict:
    return process_telemetry_file(telemetry_file_path)


def get_basic_packet_info(packet_name, packet):
    try:
        seconds = int(packet[packet_name + "_HDR_SEC"])
        microseconds = int(packet[packet_name + "_HDR_USEC"])
    except ValueError:
        seconds = 0
        microseconds = 0
        warnings.warn("Time could not be properly extracted for packet.",
                      CCSDSPacketConstructionWarning)
    timestamp = (datetime(2000, 1, 1)
                 + timedelta(seconds=seconds) + timedelta(microseconds=microseconds))

    try:
        spacecraft_id = int(packet[packet_name + "_HDR_SCID"])
    except ValueError:
        spacecraft_id = -1
        warnings.warn("Spacecraft ID could not be extracted for packet.",
                      CCSDSPacketConstructionWarning)

    try:
        flash_block_address = int(packet[packet_name + "_HDR_FLASH_BLOCK"])
    except ValueError:
        flash_block_address = -1
        warnings.warn("Flash block address could not be extracted for packet.",
                      CCSDSPacketConstructionWarning)

    return timestamp, spacecraft_id, flash_block_address


def form_packet_entry(apid, packet, packet_num, source_tlm_file_id):
    packet_name = PACKET_APID2NAME[apid]

    timestamp, spacecraft_id, flash_block_address = get_basic_packet_info(packet_name, packet)

    match packet_name.lower():
        case 'sci_xfi':
            return SciPacket(apid=apid,
                             sequence_count=packet['CCSDS_SEQUENCE_COUNT'],
                             length=packet['CCSDS_PACKET_LENGTH'],
                             spacecraft_id=spacecraft_id,
                             flash_block=flash_block_address,
                             timestamp=timestamp,
                             packet_num=packet_num,
                             source_tlm_file=source_tlm_file_id,
                             is_used=False,
                             img_pkt_grp=packet['SCI_XFI_HDR_GRP'],
                             compression_settings=packet['SCI_XFI_COM_SET'],
                             acquisition_settings=packet['SCI_XFI_ACQ_SET'],)
        case 'eng_xact':
            return EngXACTPacket(apid=apid,
                                 sequence_count=packet['CCSDS_SEQUENCE_COUNT'],
                                 length=packet['CCSDS_PACKET_LENGTH'],
                                 spacecraft_id=spacecraft_id,
                                 flash_block=flash_block_address,
                                 timestamp=timestamp,
                                 packet_num=packet_num,
                                 source_tlm_file=source_tlm_file_id,
                                 ATT_DET_Q_BODY_WRT_ECI1=packet['ATT_DET_Q_BODY_WRT_ECI1'],
                                 ATT_DET_Q_BODY_WRT_ECI2=packet['ATT_DET_Q_BODY_WRT_ECI2'],
                                 ATT_DET_Q_BODY_WRT_ECI3=packet['ATT_DET_Q_BODY_WRT_ECI3'],
                                 ATT_DET_Q_BODY_WRT_ECI4=packet['ATT_DET_Q_BODY_WRT_ECI4'],
                                 ATT_DET_RESIDUAL1=packet['ATT_DET_RESIDUAL1'],
                                 ATT_DET_RESIDUAL2=packet['ATT_DET_RESIDUAL2'],
                                 ATT_DET_RESIDUAL3=packet['ATT_DET_RESIDUAL3'],
                                 REFS_POSITION_WRT_ECI1=packet['REFS_POSITION_WRT_ECI1'],
                                 REFS_POSITION_WRT_ECI2=packet['REFS_POSITION_WRT_ECI2'],
                                 REFS_POSITION_WRT_ECI3=packet['REFS_POSITION_WRT_ECI3'],
                                 REFS_VELOCITY_WRT_ECI1=packet['REFS_VELOCITY_WRT_ECI1'],
                                 REFS_VELOCITY_WRT_ECI2=packet['REFS_VELOCITY_WRT_ECI2'],
                                 REFS_VELOCITY_WRT_ECI3=packet['REFS_VELOCITY_WRT_ECI3'],
                                 ATT_CMD_CMD_Q_BODY_WRT_ECI1=packet['ATT_CMD_CMD_Q_BODY_WRT_ECI1'],
                                 ATT_CMD_CMD_Q_BODY_WRT_ECI2=packet['ATT_CMD_CMD_Q_BODY_WRT_ECI2'],
                                 ATT_CMD_CMD_Q_BODY_WRT_ECI3=packet['ATT_CMD_CMD_Q_BODY_WRT_ECI3'],
                                 ATT_CMD_CMD_Q_BODY_WRT_ECI4=packet['ATT_CMD_CMD_Q_BODY_WRT_ECI4'],)
        case 'eng_pfw':
            return ENGPFWPacket(
                apid=apid,
                sequence_count=packet['CCSDS_SEQUENCE_COUNT'],
                length=packet['CCSDS_PACKET_LENGTH'],
                spacecraft_id=spacecraft_id,
                flash_block=flash_block_address,
                timestamp=timestamp,
                packet_num=packet_num,
                source_tlm_file=source_tlm_file_id,
                PFW_STATUS=packet['PFW_STATUS'],
                STEP_CALC=packet['STEP_CALC'],
                LAST_CMD_N_STEPS=packet['LAST_CMD_N_STEPS'],
                POSITION_CURR=packet['POSITION_CURR'],
                POSITION_CMD=packet['POSITION_CMD'],
                RESOLVER_POS_RAW=packet['RESOLVER_POS_RAW'],
                RESOLVER_POS_CORR=packet['RESOLVER_POS_CORR'],
                RESOLVER_READ_CNT=packet['RESOLVER_READ_CNT'],
                LAST_MOVE_N_STEPS=packet['LAST_MOVE_N_STEPS'],
                LAST_MOVE_EXECUTION_TIME=packet['LAST_MOVE_EXECUTION_TIME'],
                LIFETIME_STEPS_TAKEN=packet['LIFETIME_STEPS_TAKEN'],
                LIFETIME_EXECUTION_TIME=packet['LIFETIME_EXECUTION_TIME'],
                FSM_CTRL_STATE=packet['FSM_CTRL_STATE'],
                READ_SUB_STATE=packet['READ_SUB_STATE'],
                MOVE_SUB_STATE=packet['MOVE_SUB_STATE'],
                HOME_SUB_STATE=packet['HOME_SUB_STATE'],
                HOME_POSITION=packet['HOME_POSITION'],
                RESOLVER_SELECT=packet['RESOLVER_SELECT'],
                RESOLVER_TOLERANCE_HOME=packet['RESOLVER_TOLERANCE_HOME'],
                RESOLVER_TOLERANCE_CURR=packet['RESOLVER_TOLERANCE_CURR'],
                STEPPER_SELECT=packet['STEPPER_SELECT'],
                STEPPER_RATE_DELAY=packet['STEPPER_RATE_DELAY'],
                STEPPER_RATE=packet['STEPPER_RATE'],
                SHORT_MOVE_SETTLING_TIME_MS=packet['SHORT_MOVE_SETTLING_TIME_MS'],
                LONG_MOVE_SETTLING_TIME_MS=packet['LONG_MOVE_SETTLING_TIME_MS'],
                PRIMARY_STEP_OFFSET_1=packet['PRIMARY_STEP_OFFSET_1'],
                PRIMARY_STEP_OFFSET_2=packet['PRIMARY_STEP_OFFSET_2'],
                PRIMARY_STEP_OFFSET_3=packet['PRIMARY_STEP_OFFSET_3'],
                PRIMARY_STEP_OFFSET_4=packet['PRIMARY_STEP_OFFSET_4'],
                PRIMARY_STEP_OFFSET_5=packet['PRIMARY_STEP_OFFSET_5'],
                REDUNDANT_STEP_OFFSET_1=packet['REDUNDANT_STEP_OFFSET_1'],
                REDUNDANT_STEP_OFFSET_2=packet['REDUNDANT_STEP_OFFSET_2'],
                REDUNDANT_STEP_OFFSET_3=packet['REDUNDANT_STEP_OFFSET_3'],
                REDUNDANT_STEP_OFFSET_4=packet['REDUNDANT_STEP_OFFSET_4'],
                REDUNDANT_STEP_OFFSET_5=packet['REDUNDANT_STEP_OFFSET_5'],
                PRIMARY_RESOLVER_POSITION_1=packet['PRIMARY_RESOLVER_POSITION_1'],
                PRIMARY_RESOLVER_POSITION_2=packet['PRIMARY_RESOLVER_POSITION_2'],
                PRIMARY_RESOLVER_POSITION_3=packet['PRIMARY_RESOLVER_POSITION_3'],
                PRIMARY_RESOLVER_POSITION_4=packet['PRIMARY_RESOLVER_POSITION_4'],
                PRIMARY_RESOLVER_POSITION_5=packet['PRIMARY_RESOLVER_POSITION_5'],
                REDUNDANT_RESOLVER_POSITION_1=packet['REDUNDANT_RESOLVER_POSITION_1'],
                REDUNDANT_RESOLVER_POSITION_2=packet['REDUNDANT_RESOLVER_POSITION_2'],
                REDUNDANT_RESOLVER_POSITION_3=packet['REDUNDANT_RESOLVER_POSITION_3'],
                REDUNDANT_RESOLVER_POSITION_4=packet['REDUNDANT_RESOLVER_POSITION_4'],
                REDUNDANT_RESOLVER_POSITION_5=packet['REDUNDANT_RESOLVER_POSITION_5'],
            )
        case _:
            warnings.warn("Unable to add packet to database.", CCSDSPacketDatabaseUpdateWarning)
            return None

@task
def update_tlm_database(packets, tlm_id: int, session=None):
    if session is None:
        session = get_database_session()

    for apid, this_apid_packets in packets.items():
        for i in range(len(this_apid_packets['CCSDS_APID'])):
            if apid in PACKET_APID2NAME:
                try:
                    this_packet = form_packet_entry(apid, get_single_packet(this_apid_packets, i),
                                                    i, tlm_id)
                    if this_packet is not None:
                        session.add(this_packet)
                except (sqlalchemy.exc.DataError, pymysql.err.DataError) as e:
                    warnings.warn(f"Unable to add packet to database, {e}.", CCSDSPacketDatabaseUpdateWarning)
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

def get_fits_metadata(observation_time, spacecraft_id, session):
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
    return {'spacecraft_id': spacecraft_id,
            'datetime': observation_time,
            'ATT_DET_Q_BODY_WRT_ECI1': ATT_DET_Q_BODY_WRT_ECI1,
            'ATT_DET_Q_BODY_WRT_ECI2': ATT_DET_Q_BODY_WRT_ECI2,
            'ATT_DET_Q_BODY_WRT_ECI3': ATT_DET_Q_BODY_WRT_ECI3,
            'ATT_DET_Q_BODY_WRT_ECI4': ATT_DET_Q_BODY_WRT_ECI4,
            'POSITION_CURR': before_pfw.POSITION_CURR}


def form_preliminary_wcs(metadata, plate_scale):
    """Create the preliminary WCS for punchbowl"""
    quaternion = np.array([metadata['ATT_DET_Q_BODY_WRT_ECI1'] * 0.5E-10,
                           metadata['ATT_DET_Q_BODY_WRT_ECI2'] * 0.5E-10,
                           metadata['ATT_DET_Q_BODY_WRT_ECI3'] * 0.5E-10,
                           metadata['ATT_DET_Q_BODY_WRT_ECI4'] * 0.5E-10])
    ra, dec, roll = eci_quaternion_to_ra_dec(quaternion)
    projection = "ARC" if metadata['spacecraft_id'] == '4' else 'AZP'
    celestial_wcs = WCS(naxis=2)
    celestial_wcs.wcs.crpix = (1024.5, 1024.5)
    celestial_wcs.wcs.crval = (ra, dec)
    celestial_wcs.wcs.cdelt = plate_scale, plate_scale
    celestial_wcs.wcs.pc = calculate_pc_matrix(roll, celestial_wcs.wcs.cdelt)
    celestial_wcs.wcs.set_pv([(2, 1, (-sun.earth_distance(metadata['datetime']) / sun.constants.radius).decompose().value)])
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
    np.save("experiment.npy", flat_image)
    pixel_values = unpack_Nbit_values(flat_image, byteorder=">", N=16)
    nvals = pixel_values.size
    width = 2176
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
