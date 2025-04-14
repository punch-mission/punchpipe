import io
import os
import json
import traceback
from glob import glob
from typing import Any, Dict, Tuple
from datetime import UTC, datetime, timedelta

import astropy
import astropy.time
import astropy.units as u
import ccsdspy
import numpy as np
import pandas as pd
import pylibjpeg
import sqlalchemy
from astropy.coordinates import GCRS, EarthLocation, HeliocentricMeanEcliptic, SkyCoord
from astropy.time import Time
from astropy.wcs import WCS
from ccsdspy import PacketArray, PacketField, converters
from ccsdspy.utils import split_by_apid
from dateutil.parser import parse as parse_datetime_str
from ndcube import NDCube
from prefect import flow, get_run_logger, task
from prefect.blocks.core import Block
from prefect.blocks.fields import SecretDict
from prefect.cache_policies import NO_CACHE
from prefect.context import get_run_context
from punchbowl.data import NormalizedMetadata, get_base_file_name, write_ndcube_to_fits
from punchbowl.data.wcs import calculate_helio_wcs_from_celestial, calculate_pc_matrix
from sqlalchemy import Boolean, Column, Integer, String, and_, or_
from sqlalchemy.dialects.mysql import DATETIME, FLOAT, INTEGER
from sqlalchemy.orm import Session
from sunpy.coordinates import (
    HeliocentricEarthEcliptic,
    HeliocentricInertial,
    HeliographicCarrington,
    HeliographicStonyhurst,
)

from punchpipe.control.db import Base, File, Flow, PacketHistory, TLMFiles
from punchpipe.control.util import get_database_engine_session, get_database_session, load_pipeline_configuration

FIXED_PACKETS = ['ENG_XACT', 'ENG_LED', 'ENG_PFW', 'ENG_CEB']
VARIABLE_PACKETS = ['SCI_XFI']
PACKET_CADENCE = {'ENG_XACT': 10}  # only take every tenth ENG_XACT packet for 10Hz resolution

class SpacecraftMapping(Block):
    mapping: SecretDict

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

def open_and_split_packet_file(path: str) -> dict[int, io.BytesIO]:
    with open(path, "rb") as mixed_file:
        stream_by_apid = split_by_apid(mixed_file)
    return stream_by_apid

def read_tlm_defs(path):
    tlm = pd.read_excel(path, sheet_name=None)
    for sheet in tlm.keys():
        tlm[sheet] = tlm[sheet].rename(columns={c: c.strip() for c in tlm[sheet].columns})
        if "Start Byte" in tlm[sheet].columns:
            tlm[sheet]["Bit"] = tlm[sheet]["Start Byte"]*8 + tlm[sheet]["Start Bit"]
    apids = tlm["Overview"].dropna().copy()
    apids.index = [int(x.split("x")[1], 16) for x in apids["APID"]]
    apids.columns = ["Name", "APID", "Size_bytes", "Description", "Size_words", "Size_remainder"]
    apids.loc[:, "Size_bytes"] = apids["Size_bytes"].astype(int)
    return apids, tlm

def get_ccsds_data_type(sheet_type, data_size):
    if data_size > 64:
        return 'fill'
    elif sheet_type[0] == "F":
        return 'float'
    elif sheet_type[0] == "I":
        return 'int'
    elif sheet_type[0] == "U":
        return 'uint'
    else:
        return 'fill'

def get_database_data_type(sheet_type, data_size):
    if data_size > 64:
        return None
    if sheet_type[0] == "F":
        return FLOAT()
    elif sheet_type[0] == "I":
        return INTEGER()
    elif sheet_type[0] == "U":
        return INTEGER(unsigned=True)
    else:
        return None

def create_class(table_name, columns):
    attrs = {'__tablename__': table_name}
    for name, col_type in columns.items():
        attrs[name] = Column(col_type,
                             primary_key=(name == 'id'), # Assuming 'id' is primary key if present
                             index=(name == 'timestamp' or name == 'id'))
    return type(table_name, (Base,), attrs)


@task(cache_policy=NO_CACHE)
def create_database_from_tlm(tlm, engine):
    REQUIRED_COLUMNS = ['id', 'tlm_id', 'packet_index',
                        'CCSDS_VERSION_NUMBER', 'CCSDS_PACKET_TYPE', 'CCSDS_SECONDARY_FLAG',
                        'CCSDS_APID', 'CCSDS_SEQUENCE_FLAG', 'CCSDS_SEQUENCE_COUNT', 'CCSDS_PACKET_LENGTH']
    database_classes = {}
    for packet_name in FIXED_PACKETS:
        columns = {name: INTEGER(unsigned=True) for name in REQUIRED_COLUMNS}
        columns['timestamp'] = DATETIME(fsp=6)
        for i, row in tlm[packet_name].iterrows():
            if i > 6:
                # we set the database type to None if it's not a valid kind for the database
                columns[row['Mnemonic']] = get_database_data_type(row['Type'], row['Data Size'])
        # thus we purge the columns of invalid kinds after
        columns = {k: v for k, v in columns.items() if v is not None}

        # ENG_LED has some extra times that we want to encode...
        # we don't have a better way other than manually do this right now
        if packet_name == "ENG_LED":
            columns['LED_START_TIME'] = DATETIME(fsp=6)
            columns['LED_END_TIME'] = DATETIME(fsp=6)

        database_classes[packet_name] = create_class(packet_name, columns)

    for packet_name in VARIABLE_PACKETS:
        num_fields = len(tlm[packet_name])
        columns = {name: INTEGER(unsigned=True) for name in REQUIRED_COLUMNS}
        columns['timestamp'] = DATETIME(fsp=6)
        columns['is_used'] = Boolean
        columns['num_attempts'] = Integer
        columns['last_attempt'] = DATETIME(fsp=6)
        columns['last_skip_reason'] = String(300)
        for i, row in tlm[packet_name].iterrows():
            if i > 6 and i != num_fields - 1:  # the expanding packet is assumed to be last so we skip it
                # we set the database type to None if it's not a valid kind for the database
                columns[row['Mnemonic']] = get_database_data_type(row['Type'], row['Data Size'])
        # thus we purge the columns of invalid kinds after
        columns = {k: v for k, v in columns.items() if v is not None}
        database_classes[packet_name] = create_class(packet_name, columns)

    Base.metadata.create_all(engine)
    return database_classes

def create_packet_definitions(tlm, parse_expanding_fields=True):
    defs = {}
    for packet_name in FIXED_PACKETS:
        fields = []
        for i, row in tlm[packet_name].iterrows():
            if i > 6:  # CCSDSPy doesn't need the primary header, but it's in the .xls file, so we skip
                fields.append(PacketField(name=row['Mnemonic'],
                                          data_type=get_ccsds_data_type(row['Type'], row['Data Size']),
                                          bit_length=row['Data Size']))
        pkt = ccsdspy.FixedLength(fields)

        pkt.add_converted_field(
            (f'{packet_name}_HDR_SEC', f'{packet_name}_HDR_USEC'),
            'timestamp',
            converters.DatetimeConverter(
                since=datetime(2000, 1, 1),
                units=('seconds', 'microseconds')
            )
        )

        if packet_name=="ENG_LED":
            # LED packets have extra times... so we'll just convert them here
            pkt.add_converted_field(
                ('LED_PLS_START_SEC', 'LED_PLS_START_USEC'),
                'LED_START_TIME',
                converters.DatetimeConverter(
                    since=datetime(2000, 1, 1),
                    units=('seconds', 'microseconds')
                )
            )
            pkt.add_converted_field(
                ('LED_PLS_END_SEC', 'LED_PLS_END_USEC'),
                'LED_END_TIME',
                converters.DatetimeConverter(
                    since=datetime(2000, 1, 1),
                    units=('seconds', 'microseconds')
                )
            )

        defs[packet_name] = pkt

    for packet_name in VARIABLE_PACKETS:
        fields = []
        num_fields = len(tlm[packet_name])
        for i, row in tlm[packet_name].iterrows():
            if i > 6 and i != num_fields - 1:  # the expanding packet is assumed to be last
                fields.append(PacketField(name=row['Mnemonic'],
                                          data_type=get_ccsds_data_type(row['Type'], row['Data Size']),
                                          bit_length=row['Data Size']))
            elif i == num_fields - 1 and parse_expanding_fields:
                fields.append(PacketArray(name=row['Mnemonic'],
                                          data_type='uint',
                                          bit_length=8,
                                          array_shape="expand"))
        pkt = ccsdspy.VariableLength(fields)

        pkt.add_converted_field(
            (f'{packet_name}_HDR_SEC', f'{packet_name}_HDR_USEC'),
            'timestamp',
            converters.DatetimeConverter(
                since=datetime(2000, 1, 1),
                units=('seconds', 'microseconds')
            )
        )

        defs[packet_name] = pkt
    return defs

@task(cache_policy=NO_CACHE)
def detect_new_tlm_files(pipeline_config: dict, session=None) -> [str]:
    if session is None:
        session = get_database_session()

    tlm_directory = pipeline_config['tlm_directory']
    found_tlm_files = list(glob(os.path.join(tlm_directory, '**/*.tlm'), recursive=True))

    # drop all files before the 'tlm_start_date'
    if 'tlm_start_date' in pipeline_config:
        tlm_start_date = parse_datetime_str(pipeline_config['tlm_start_date'])
        found_tlm_file_dates = [datetime.strptime("_".join(os.path.basename(path).split("_")[3:-1]),
                                                  "%Y_%j_%H_%M")
                                for path in found_tlm_files]
        found_tlm_files = [path for path, date in zip(found_tlm_files, found_tlm_file_dates)
                               if date > tlm_start_date]
    found_tlm_files = set(found_tlm_files)
    database_tlm_files = set([p[0] for p in session.query(TLMFiles.path).distinct().all()])

    return sorted(list(found_tlm_files - database_tlm_files))

@task(cache_policy=NO_CACHE)
def process_telemetry_file(path, defs, apid_name2num, logger):
    success = True
    contents = open_and_split_packet_file(path)
    parsed = {}
    for packet_name in defs:
        apid_num = apid_name2num[packet_name]
        if apid_num in contents:
            logger.info(f"Parsing {packet_name}")
            try:
                parsed[packet_name] = defs[packet_name].load(contents[apid_num], include_primary_header=True)
            except (ValueError, RuntimeError):
                success = False
                logger.error(f"Failed parsing {packet_name} on {path}")
                logger.error(traceback.format_exc())
        else:
            logger.info(f"{packet_name} not found")
    return parsed, success

@task(cache_policy=NO_CACHE)
def ingest_tlm_file(path: str, session: Session,
                    defs: dict[str, ccsdspy.VariableLength | ccsdspy.FixedLength],
                    apid_name2num: dict[str, int],
                    database_classes: dict[str, Base]):
    logger = get_run_logger()

    logger.info(f"Ingesting {path}")
    tlm_db_entry = TLMFiles(
        path=path,
        successful=False,
        num_attempts=0,
        last_attempt=datetime.now(UTC)
    )
    session.add(tlm_db_entry)
    session.commit()

    parsed, success = process_telemetry_file(path, defs, apid_name2num, logger)

    logger.debug("Adding parsed packets to database")
    for packet_name in parsed:
        packet_keys = set(database_classes[packet_name].__table__.columns.keys()) &  set(parsed[packet_name].keys())
        num_packets = len(parsed[packet_name]['CCSDS_APID'])
        packet_numbers_used = list(range(0, num_packets, PACKET_CADENCE.get(packet_name, 1)))
        pkts = {i: {} for i in packet_numbers_used}
        try:
            logger.info(f"Adding {len(packet_numbers_used)} packets for {packet_name} from {path}")
            for packet_num in packet_numbers_used:
                for key in packet_keys:
                    pkts[packet_num][key] = parsed[packet_name][key][packet_num]
                pkts[packet_num]['packet_index'] = packet_num
                pkts[packet_num]['tlm_id'] = tlm_db_entry.tlm_id

            # session.bulk_save_objects(pkts)
            logger.debug("INSERTING NOW")
            session.execute(
                database_classes[packet_name].__table__.insert(),
                list(pkts.values())
            )
            session.commit()
        except (ValueError, sqlalchemy.exc.DataError):
            success = False
            logger.error(f"Failed adding {packet_name} from {path} to database")
            logger.error(traceback.format_exc())
            session.rollback()

    tlm_db_entry.successful = success
    tlm_db_entry.num_attempts += 1
    tlm_db_entry.last_attempt = datetime.now(UTC)
    session.commit()

@task
def unpack_n_bit_values(packed: bytes, byteorder: str, n_bits=19) -> np.ndarray:
    logger = get_run_logger()
    if n_bits in (8, 16, 32, 64):
        trailing = len(packed)%(n_bits//8)
        if trailing:
            logger.info(f"Truncating {trailing} extra bytes")
            packed = packed[:-trailing]
        return np.frombuffer(packed, dtype=np.dtype(f"u{n_bits//8}").newbyteorder(byteorder))
    bit_length = len(packed)*8
    bytes_as_ints = np.frombuffer(packed, "u1")
    results = []
    for bit in range(0, bit_length, n_bits):
        encompassing_bytes = bytes_as_ints[bit//8:-((bit+n_bits)//-8)]
        # "ceil" equivalent of a//b is -(-a//b), because of
        # http://python-history.blogspot.com/2010/08/why-pythons-integer-division-floors.html
        if len(encompassing_bytes)*8 < n_bits:
            logger.info(f"Terminating at bit {bit} because there are only {len(encompassing_bytes)*8}"
                      f" bits left, which is not enough to make a {n_bits}-bit value.")
            break
        bit_within_byte = bit % 8
        bytes_value = 0
        if byteorder in ("little", "<"):
            bytes_value = int.from_bytes(encompassing_bytes, "little")
            bits_value = (bytes_value >> bit_within_byte) & (2**n_bits - 1)
        elif byteorder in ("big", ">"):
            extra_bits_to_right = len(encompassing_bytes)*8 - (bit_within_byte+n_bits)
            bytes_value = int.from_bytes(encompassing_bytes, "big")
            bits_value = (bytes_value >> extra_bits_to_right) & (2**n_bits - 1)
        else:
            raise ValueError("`byteorder` must be either 'little' or 'big'")
        results.append(bits_value)
    return np.asanyarray(results)

def organize_pfw_fits_keywords(pfw_packet):
    return {
        'PFWSTAT': pfw_packet.PFW_STATUS,
        'STEPCALC': pfw_packet.STEP_CALC,
        'CMDSTEPS': pfw_packet.LAST_CMD_N_STEPS,
        'HOMEOVRD': pfw_packet.HOME_POSITION_OVRD,
        'POSCURR': pfw_packet.POSITION_CURR,
        'POSCMD': pfw_packet.POSITION_CMD,
        'POSRAW': pfw_packet.RESOLVER_POS_RAW,
        'POSRAW2': pfw_packet.RESOLVER_POS_CORR,
        'READCNT': pfw_packet.RESOLVER_READ_CNT,
        'LMNSTEP': pfw_packet.LAST_MOVE_N_STEPS,
        'LMTIME': pfw_packet.LAST_MOVE_EXECUTION_TIME,
        'LTSTEP': pfw_packet.LIFETIME_STEPS_TAKEN,
        'LTTIME': pfw_packet.LIFETIME_EXECUTION_TIME,
        'FSMSTAT': pfw_packet.FSM_CTRL_STATE,
        'READSTAT': pfw_packet.READ_SUB_STATE,
        'MOVSTAT': pfw_packet.MOVE_SUB_STATE,
        'HOMESTAT': pfw_packet.HOME_SUB_STATE,
        'HOMEPOS': pfw_packet.HOME_POSITION,
        'RESSEL': pfw_packet.RESOLVER_SELECT,
        'RESTOLH': pfw_packet.RESOLVER_TOLERANCE_HOME,
        'RESTOLC': pfw_packet.RESOLVER_TOLERANCE_CURR,
        'STEPSEL': pfw_packet.STEPPER_SELECT,
        'STEPDLY': pfw_packet.STEPPER_RATE_DELAY,
        'STEPRATE': pfw_packet.STEPPER_RATE,
        'SHORTMV': pfw_packet.SHORT_MOVE_SETTLING_TIME_MS,
        'LONGMV': pfw_packet.LONG_MOVE_SETTLING_TIME_MS,
        'PFWOFF1': pfw_packet.PRIMARY_STEP_OFFSET_1,
        'PFWOFF2': pfw_packet.PRIMARY_STEP_OFFSET_2,
        'PFWOFF3': pfw_packet.PRIMARY_STEP_OFFSET_3,
        'PFWOFF4': pfw_packet.PRIMARY_STEP_OFFSET_4,
        'PFWOFF5': pfw_packet.PRIMARY_STEP_OFFSET_5,
        'RPFWOFF1': pfw_packet.REDUNDANT_STEP_OFFSET_1,
        'RPFWOFF2': pfw_packet.REDUNDANT_STEP_OFFSET_2,
        'RPFWOFF3': pfw_packet.REDUNDANT_STEP_OFFSET_3,
        'RPFWOFF4': pfw_packet.REDUNDANT_STEP_OFFSET_4,
        'RPFWOFF5': pfw_packet.REDUNDANT_STEP_OFFSET_5,
        'PFWPOS1': pfw_packet.PRIMARY_RESOLVER_POSITION_1,
        'PFWPOS2': pfw_packet.PRIMARY_RESOLVER_POSITION_2,
        'PFWPOS3': pfw_packet.PRIMARY_RESOLVER_POSITION_3,
        'PFWPOS4': pfw_packet.PRIMARY_RESOLVER_POSITION_4,
        'PFWPOS5': pfw_packet.PRIMARY_RESOLVER_POSITION_5,
        'RPFWPOS1': pfw_packet.REDUNDANT_RESOLVER_POSITION_1,
        'RPFWPOS2': pfw_packet.REDUNDANT_RESOLVER_POSITION_2,
        'RPFWPOS3': pfw_packet.REDUNDANT_RESOLVER_POSITION_3,
        'RPFWPOS4': pfw_packet.REDUNDANT_RESOLVER_POSITION_4,
        'RPFWPOS5': pfw_packet.REDUNDANT_RESOLVER_POSITION_5
    }

def organize_led_fits_keywords(led_packet):
    return {
        'LED1STAT': led_packet.LED1_ACTIVE_STATE,
        'LEDPLSN': led_packet.LED_CFG_NUM_PLS,
        'LED2STAT': led_packet.LED2_ACTIVE_STATE,
        'LEDPLSD': led_packet.LED_CFG_PLS_DLY,
        'LEDPLSW': led_packet.LED_CFG_PLS_WIDTH,
    }


def organize_ceb_fits_keywords(ceb_packet):
    return {'CEBSTAT': ceb_packet.CEB_STATUS_REG,
            'CEBTIME': ceb_packet.CEB_STATUS_REG_SPW_TIMECODE,
            'CEBWGS': ceb_packet.WGS_STATUS,
            'CEBFIFO': ceb_packet.VIDEO_FIFO_STATUS,
            'CEBBIAS1': ceb_packet.CCD_OUTPUT_DRAIN_BIAS,
            'CEBBIAS2': ceb_packet.CCD_DUMP_DRAIN_BIAS,
            'CEBBIAS3': ceb_packet.CCD_RESET_DRAIN_BIAS,
            'CEBBIAS4': ceb_packet.CCD_TOP_GATE_BIAS,
            'CEBBIAS5': ceb_packet.CCD_OUTPUT_GATE_BIAS,
            'CEBVREF': ceb_packet.VREF_P2_5V1,
            'CEBGND1': ceb_packet.GROUND1,
            'CEBCONV1': ceb_packet.DCDC_CONV_P30V_OUT,
            'CEBCONV2': ceb_packet.DCDC_CONV_P15V_OUT,
            'CEBCONV3': ceb_packet.DCDC_CONV_P5V_OUT,
            'BIASVREF': ceb_packet.VREF_BIAS,
            'CEBGND2': ceb_packet.GROUND2,
            'CEBSEDAC': ceb_packet.IPF_SBE_CNT,
            'CEBMEDAC': ceb_packet.IPF_MBE_CNT}


def organize_spacecraft_position_keywords(observation_time, before_xact, after_xact):
    position = EarthLocation.from_geocentric(before_xact.GPS_POSITION_ECEF1*2E-5*u.km,
                                             before_xact.GPS_POSITION_ECEF2*2E-5*u.km,
                                             before_xact.GPS_POSITION_ECEF3*2E-5*u.km)

    location = EarthLocation.from_geodetic(position.geodetic.lon.deg,
                                           position.geodetic.lat.deg,
                                           position.geodetic.height.to(u.m).value)
    obstime = Time(observation_time)

    # Convert to GCRS frame
    gcrs = GCRS(location.get_itrs(obstime).cartesian, obstime=obstime)

    # HCI (Heliocentric Inertial)
    hci = gcrs.transform_to(HeliocentricInertial(obstime=obstime))

    # (Heliocentric Earth Ecliptic)
    hee = gcrs.transform_to(HeliocentricEarthEcliptic(obstime=obstime))

    # HAE (Heliocentric Aries Ecliptic)
    hae = gcrs.transform_to(HeliocentricMeanEcliptic(obstime=obstime))

    # HEQ (Heliocentric Earth Equatorial)
    heq = gcrs.transform_to(HeliographicStonyhurst(obstime=obstime))

    # Carrington coordinates
    carrington = gcrs.transform_to(HeliographicCarrington(obstime=obstime, observer='self'))

    return {
        "HCIX_OBS": hci.cartesian.x.to(u.m).value,
        "HCIY_OBS": hci.cartesian.y.to(u.m).value,
        "HCIZ_OBS": hci.cartesian.z.to(u.m).value,
        "HEEX_OBS": hee.cartesian.x.to(u.m).value,
        "HEEY_OBS": hee.cartesian.y.to(u.m).value,
        "HEEZ_OBS": hee.cartesian.z.to(u.m).value,
        "HAEX_OBS": hae.cartesian.x.to(u.m).value,
        "HAEY_OBS": hae.cartesian.y.to(u.m).value,
        "HAEZ_OBS": hae.cartesian.z.to(u.m).value,
        "HEQX_OBS": heq.cartesian.x.to(u.m).value,
        "HEQY_OBS": heq.cartesian.y.to(u.m).value,
        "HEQZ_OBS": heq.cartesian.z.to(u.m).value,
        "CRLT_OBS": carrington.lat.deg,
        "CRLN_OBS": carrington.lon.deg,
        'GEOD_LAT': position.geodetic.lat.deg,
        'GEOD_LON': position.geodetic.lon.deg,
        'GEOD_ALT': position.geodetic.height.to(u.m).value
    }

def organize_compression_and_acquisition_settings(compression_settings, acquisition_settings):
    return {"SCALE": float(compression_settings['SCALE']),
            "PMB_INIT": compression_settings['PMB_INIT'],
            "CMP_BYP": compression_settings['CMP_BYP'],
            "BSEL": compression_settings['BSEL'],
            "ISSQRT": compression_settings['SQRT'],
            "WASJPEG": compression_settings['JPEG'],
            "ISTEST": compression_settings['TEST'],
            "DELAY": acquisition_settings['DELAY'],
            "IMGCOUNT": acquisition_settings['IMG_NUM']+1,
            "EXPTIME": acquisition_settings['EXPOSURE']/10.0,
            "TABLE1": acquisition_settings['TABLE1'],
            "TABLE2": acquisition_settings['TABLE2']}

def organize_gain_info(spacecraft_id):
    match spacecraft_id:
        case 0x2F:
            gains = {'GAINLEFT': 4.98,'GAINRGHT': 4.92}
        case 0x10:
            gains = {'GAINLEFT': 4.93, 'GAINRGHT': 4.90}
        case 0x2C:
            gains = {'GAINLEFT': 4.90, 'GAINRGHT': 5.04}
        case 0xF9:
            gains = {'GAINLEFT': 4.94, 'GAINRGHT': 4.89}
        case _:
            gains = {'GAINLEFT': 4.9, 'GAINRGHT': 4.9}
    return gains

def check_for_full_image(img_packets):
    n_starts = img_packets.count(b'\xFF\xD8')
    n_endings = img_packets.count(b'\xFF\xD9')
    if n_starts != n_endings:
        raise RuntimeError("Image does not have both start and end indicators")

@task
def decode_image_packets(img_packets, compression_settings):
    # check_for_full_image(img_packets)
    if compression_settings["JPEG"]: # JPEG bit enabled (upper two pathways)
        if compression_settings["CMP_BYP"]: # skipped actual JPEG-ification
            pixel_values = unpack_n_bit_values(img_packets, byteorder=">", n_bits=16)
            # either 12-bit values, but placed into 16b words where the 4 MSb are 0000; or 16-bit truncated pixel values
        else: # data is in JPEG-LS format
            pixel_values: np.ndarray = pylibjpeg.decode(img_packets.tobytes())
    else:
        pixel_values = unpack_n_bit_values(img_packets, byteorder="<", n_bits=19)
    if pixel_values.max() < 2**16:
        pixel_values = pixel_values.astype(np.uint16)
    else:
        pixel_values = pixel_values.astype(np.uint32)

    num_vals = pixel_values.size
    width = 2176 if num_vals > 2048 * 2048 else 2048
    if num_vals % width == 0:
        return pixel_values.reshape((-1, width))
    else:
        return np.ravel(pixel_values)[:width*(num_vals//width)].reshape((-1, width))

PFW_POSITIONS = {"M": 960,
                 "opaque": 720,
                 "Z": 480,
                 "P": 240,
                 "Clear": 0}

POSITIONS_TO_CODES = {"Clear": "CR", "P": "PP", "M": "PM", "Z": "PZ"}

PFW_POSITION_MAPPING = ["Manual", "M", "opaque", "Z", "P", "Clear"]


def determine_file_type(polarizer_position, led_info, image_shape) -> str:
    if led_info is not None:
        return "DY"
    elif image_shape != (2048, 2048):
        return "OV"
    elif polarizer_position == 0 or polarizer_position == 2:  # position = 0 is manual pointing. position = 2 is opaque
        return "DK"
    else:
        return POSITIONS_TO_CODES[PFW_POSITION_MAPPING[polarizer_position]]


def get_metadata(db_classes, first_image_packet, image_shape, session, logger) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    acquisition_settings  = unpack_acquisition_settings(first_image_packet.SCI_XFI_HDR_ACQ_SET)
    compression_settings  = unpack_compression_settings(first_image_packet.SCI_XFI_HDR_COM_SET)

    observation_time = first_image_packet.timestamp
    spacecraft_id = first_image_packet.SCI_XFI_HDR_SCID

    exposure_time = acquisition_settings['EXPOSURE'] / 10  # exptime in seconds since it's reported in ticks of 100ms

    logger.debug("Getting XACT position")
    # get the XACT packet right before and right after the first image packet to determine position
    before_xact = (session.query(db_classes['ENG_XACT'])
                   .filter(db_classes['ENG_XACT'].ENG_XACT_HDR_SCID == spacecraft_id)
                   .filter(db_classes['ENG_XACT'].timestamp < observation_time)
                   .order_by(db_classes['ENG_XACT'].timestamp.desc()).first())
    after_xact = (session.query(db_classes['ENG_XACT'])
                  .filter(db_classes['ENG_XACT'].ENG_XACT_HDR_SCID == spacecraft_id)
                  .filter(db_classes['ENG_XACT'].timestamp > observation_time)
                  .order_by(db_classes['ENG_XACT'].timestamp.asc()).first())
    logger.debug("XACT retrieved")

    # TODO: slerp instead of interpolate
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

    # get the PFW packet right before the observation
    best_pfw = (session.query(db_classes['ENG_PFW'])
                  .filter(db_classes['ENG_PFW'].ENG_PFW_HDR_SCID == spacecraft_id)
                  .filter(db_classes['ENG_PFW'].timestamp < observation_time)
                  .order_by(db_classes['ENG_PFW'].timestamp.desc()).first())

    # get the CEB packet right before the observation
    best_ceb = (session.query(db_classes['ENG_CEB'])
                  .filter(db_classes['ENG_CEB'].ENG_CEB_HDR_SCID == spacecraft_id)
                  .filter(db_classes['ENG_CEB'].timestamp < observation_time)
                  .order_by(db_classes['ENG_CEB'].timestamp.desc()).first())

    # get the LED packet that corresponds to this observation if one exists
    # this is slightly different, we look for an LED packet with a start time and an end time that overlaps
    # with the observation... there is likely not one so this will be None
    best_led = (session.query(db_classes['ENG_LED'])
                .filter(db_classes['ENG_LED'].ENG_LED_HDR_SCID == spacecraft_id)
                .filter(db_classes['ENG_LED'].LED_START_TIME > observation_time)
                .filter(db_classes['ENG_LED'].LED_END_TIME < observation_time + timedelta(seconds=exposure_time))
                .first())

    position_info = {'spacecraft_id': spacecraft_id,
            'datetime': observation_time,
            'ATT_DET_Q_BODY_WRT_ECI1': ATT_DET_Q_BODY_WRT_ECI1,
            'ATT_DET_Q_BODY_WRT_ECI2': ATT_DET_Q_BODY_WRT_ECI2,
            'ATT_DET_Q_BODY_WRT_ECI3': ATT_DET_Q_BODY_WRT_ECI3,
            'ATT_DET_Q_BODY_WRT_ECI4': ATT_DET_Q_BODY_WRT_ECI4,
            'PFW_POSITION_CURR': best_pfw.POSITION_CURR}

    # fill in all the FITS info
    fits_info = {'TYPECODE': determine_file_type(best_pfw.POSITION_CURR,
                                                 best_led,
                                                 image_shape)}

    if best_pfw is not None:
        fits_info |= organize_pfw_fits_keywords(best_pfw)

    if best_led is not None:
        fits_info |= organize_led_fits_keywords(best_led)

    if best_ceb is not None:
        fits_info |= organize_ceb_fits_keywords(best_ceb)

    logger.debug("Getting spacecraft location")
    if before_xact is not None and after_xact is not None:
        fits_info |= organize_spacecraft_position_keywords(observation_time, before_xact, after_xact)
    logger.debug("Spacecraft location determined")

    fits_info |= organize_compression_and_acquisition_settings(compression_settings, acquisition_settings)

    fits_info |= organize_gain_info(spacecraft_id)

    fits_info['EXPTIME'] = acquisition_settings['EXPOSURE'] / 10.0
    fits_info['COM_SET'] = first_image_packet.SCI_XFI_HDR_COM_SET
    fits_info['ACQ_SET'] = first_image_packet.SCI_XFI_HDR_ACQ_SET
    fits_info['DATE-BEG'] = first_image_packet.timestamp.isoformat()
    date_end = first_image_packet.timestamp + timedelta(seconds=fits_info['EXPTIME'])
    fits_info['DATE-END'] = date_end.isoformat()
    date_avg =  first_image_packet.timestamp + (date_end - first_image_packet.timestamp) / 2
    fits_info['DATE-AVG'] = date_avg.isoformat()
    fits_info['DATE-OBS'] = date_avg.isoformat()
    fits_info['DATE'] = datetime.now(UTC).isoformat()

    return position_info, fits_info


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

@task
def level0_form_images(session, pipeline_config, db_classes, defs, apid_name2num):
    logger = get_run_logger()

    SCI_XFI = db_classes["SCI_XFI"]  # convenience to shorten the database access everywhere

    distinct_spacecraft = (session.query(SCI_XFI.SCI_XFI_HDR_SCID)
                           .filter(or_(~SCI_XFI.is_used, SCI_XFI.is_used.is_(None)))
                           .distinct()
                           .all())

    already_parsed_tlms = {} # path to TLM file mapped to the contents of it, we cache this to avoid re-parsing

    skip_count, success_count = 0, 0
    replay_needs = []
    for spacecraft in distinct_spacecraft:
        distinct_times = (session.query(SCI_XFI.timestamp)
                          .filter(or_(~SCI_XFI.is_used, SCI_XFI.is_used.is_(None)))
                          .filter(SCI_XFI.SCI_XFI_HDR_SCID == spacecraft[0])
                          .distinct()
                          .all())

        for t in distinct_times:
            logger.info(f"Processing spacecraft={spacecraft[0]} at time={t[0]}")
            skip_image, skip_reason = False, ""
            image_packets_entries = (session.query(SCI_XFI)
                                     .filter(and_(SCI_XFI.timestamp == t[0],
                                                 SCI_XFI.SCI_XFI_HDR_SCID == spacecraft[0]))
                                     .all())
            logger.info(f"len(packets) = {len(image_packets_entries)}")
            image_compression = [unpack_compression_settings(packet.SCI_XFI_HDR_COM_SET)
                                 for packet in image_packets_entries]
            logger.info(f"image_compression = {image_compression[0]}")

            # Determine all the relevant TLM files
            needed_tlm_ids = set([image_packet.tlm_id for image_packet in image_packets_entries])
            tlm_id_to_tlm_path = {tlm_id: session.query(TLMFiles.path).where(TLMFiles.tlm_id == tlm_id).one().path
                                  for tlm_id in needed_tlm_ids}
            needed_tlm_paths = list(session.query(TLMFiles.path).where(TLMFiles.tlm_id.in_(needed_tlm_ids)).all())
            needed_tlm_paths = [p.path for p in needed_tlm_paths]
            logger.info(f"Will use data from {needed_tlm_paths}")

            # parse any TLM files needed that haven't already been loaded for image creation
            for tlm_path in needed_tlm_paths:
                if tlm_path not in already_parsed_tlms:
                    logger.debug(f"Loading {tlm_path}...")
                    parsed_contents, success = process_telemetry_file(tlm_path, defs, apid_name2num, logger)
                    if success:
                        logger.debug(f"Successfully loaded {tlm_path}")
                        already_parsed_tlms[tlm_path] = parsed_contents
                    else:
                        logger.error(f"Failed to load {tlm_path}")
                        skip_image = True
                        skip_reason = "Could not load all needed TLM files"


            if not skip_image:
                # make it easy to grab the right TLM files in the right order
                tlm_contents = [already_parsed_tlms[tlm_path] for tlm_path in needed_tlm_paths]

                # we want to get the packet contents and order them so an image can be made
                # order the packets in the correct order for de-commutation
                order_dict = {}
                packet_entry_mapping = {}
                for packet_entry in image_packets_entries:
                    sequence_count = packet_entry.CCSDS_SEQUENCE_COUNT
                    if sequence_count in order_dict:
                        order_dict[sequence_count].append(packet_entry.id)
                    else:
                        order_dict[sequence_count] = [packet_entry.id]
                    packet_entry_mapping[packet_entry.id] = packet_entry

                # sometimes there are replays so there are repeated packets
                # we use the packet with the largest packet_id because it's most likely the newest
                ordered_image_content = []
                sequence_counter = []
                for sequence_count in sorted(list(order_dict.keys())):
                    best_packet = max(order_dict[sequence_count])
                    packet_entry = packet_entry_mapping[best_packet]
                    tlm_content_index = needed_tlm_paths.index(tlm_id_to_tlm_path[packet_entry.tlm_id])
                    selected_tlm_contents = tlm_contents[tlm_content_index]
                    ordered_image_content.append(selected_tlm_contents['SCI_XFI']['SCI_XFI_IMG_DATA'][packet_entry.packet_index])
                    sequence_counter.append(selected_tlm_contents['SCI_XFI']['SCI_XFI_HDR_IMG_PKT_GRP'][packet_entry.packet_index])

                # we check that the packets are in order now... if they're not we'll skip
                # we know a packet sequence is in order if the difference in the pkt_grp is either 1 or 255
                # 1 is the nominal case
                # 255 indicates the packets rolled over in the 8 bit counter
                sequence_counter_diff = np.diff(np.array(sequence_counter))
                if not np.all(np.isin(sequence_counter_diff, [1, 255])):
                    logger.error("Packets are out of order so skipping")
                    skip_image = True
                    skip_reason = "Packets are out of order"

                    # if this is the case, then we need a replay. So we'll log that
                    # TODO: we might be missing the first or last packet... so case the replay length should be longer
                    replay_needs.append({
                        'spacecraft': spacecraft[0],
                        'start_time': image_packets_entries[0].timestamp.isoformat(),
                        'start_block': image_packets_entries[0].SCI_XFI_HDR_FLASH_BLOCK,
                        'replay_length': image_packets_entries[-1].SCI_XFI_HDR_FLASH_BLOCK
                                         - image_packets_entries[0].SCI_XFI_HDR_FLASH_BLOCK + 1})

                # we'll finally try to decompress the image, if it fails we cannot make the image so we proceed
                try:
                    compression_settings = unpack_compression_settings(image_packets_entries[0].SCI_XFI_HDR_COM_SET)
                    image = decode_image_packets(np.concatenate(ordered_image_content), compression_settings)
                    if image.shape != (2048, 2048) and image.shape != (4192, 2176):
                        skip_image = True
                        skip_reason = f"Image is wrong shape. Found {image.shape}"
                        logger.error(skip_reason)
                        replay_needs.append({
                            'spacecraft': spacecraft[0],
                            'start_time': image_packets_entries[0].timestamp.isoformat(),
                            'start_block': image_packets_entries[0].SCI_XFI_HDR_FLASH_BLOCK,
                            'replay_length': image_packets_entries[-1].SCI_XFI_HDR_FLASH_BLOCK
                                             - image_packets_entries[0].SCI_XFI_HDR_FLASH_BLOCK + 1})
                except (ValueError, RuntimeError):
                    skip_image = True
                    skip_reason = "Image decoding failed"
                    logger.error("Could not make image")
                    logger.error(traceback.format_exc())
                    replay_needs.append({
                        'spacecraft': spacecraft[0],
                        'start_time': image_packets_entries[0].timestamp.isoformat(),
                        'start_block': image_packets_entries[0].SCI_XFI_HDR_FLASH_BLOCK,
                        'replay_length': image_packets_entries[-1].SCI_XFI_HDR_FLASH_BLOCK
                                         - image_packets_entries[0].SCI_XFI_HDR_FLASH_BLOCK + 1})


            # now that we have the image we're ready to collect the metadat and write it to file
            if not skip_image:
                try:
                    # we need to work out the SOC spacecraft ID from the MOC spacecraft id
                    spacecraft_secrets = SpacecraftMapping.load("spacecraft-ids").mapping.get_secret_value()
                    moc_index = spacecraft_secrets["moc"].index(image_packets_entries[0].SCI_XFI_HDR_SCID)
                    soc_spacecraft_id = spacecraft_secrets["soc"][moc_index]
                    logger.debug("Getting metadata for file")
                    position_info, fits_info = get_metadata(db_classes,
                                                            image_packets_entries[0],
                                                            image.shape,
                                                            session, logger)
                    logger.debug("Metadata retrieved")
                    file_type = fits_info["TYPECODE"]
                    preliminary_wcs = form_preliminary_wcs(position_info,
                                                           float(pipeline_config['plate_scale'][str(soc_spacecraft_id)]))

                    # we're ready to pack this into an NDCube to write as a FITS file using punchbowl
                    meta = NormalizedMetadata.load_template(file_type + str(soc_spacecraft_id), "0")
                    for meta_key, meta_value in fits_info.items():
                        meta[meta_key] = meta_value
                    cube = NDCube(data=image, meta=meta, wcs=preliminary_wcs)

                    # we also need to add it to the database
                    l0_db_entry = File(level="0",
                                       file_type=file_type,
                                       observatory=str(soc_spacecraft_id),
                                       file_version="1",  # TODO: increment the file version
                                       software_version="TODO",
                                       date_created=datetime.now(UTC),
                                       date_obs=t[0],
                                       date_beg=t[0],
                                       date_end=t[0],
                                       state="created")

                    # finally time to write to file
                    out_path =  os.path.join(l0_db_entry.directory(pipeline_config['root']),
                                             get_base_file_name(cube)) + ".fits"
                    os.makedirs(os.path.dirname(out_path), exist_ok=True)
                    session.add(l0_db_entry)
                    session.commit()
                    logger.info(f"Writing to {out_path}")
                    write_ndcube_to_fits(cube, out_path, overwrite=True)
                    success_count += 1
                except Exception as e:
                    skip_image = True
                    skip_reason = "Could not make metadata and write image"
                    logger.error(f"Failed writing image because: {e}")
                    logger.error(traceback.format_exc())
                    skip_count += 1
            else:
                skip_count += 1

            # go back and do some clean up if we skipped the image
            if skip_image:
                now = datetime.now(UTC)
                for packet in image_packets_entries:
                    packet.is_used = False
                    packet.num_attempts = packet.num_attempts + 1 if packet.num_attempts is not None else 1
                    packet.last_attempt = now
                    packet.last_skip_reason = skip_reason
            else:
                now = datetime.now(UTC)
                for packet in image_packets_entries:
                    packet.is_used = True
                    packet.num_attempts = packet.num_attempts + 1 if packet.num_attempts is not None else 1
                    packet.last_attempt = now
            session.commit()

        history = PacketHistory(datetime=datetime.now(UTC),
                               num_images_succeeded=success_count,
                               num_images_failed=skip_count)
        session.add(history)
        session.commit()
        logger.info(f"SUCCESS={success_count}")
        logger.info(f"FAILURE={skip_count}")

    # Split into multiple files and append updates instead of making a new file each time
    # We label not with the spacecraft telemetry ID but with the spelled out name
    all_replays = pd.DataFrame(replay_needs)
    for df_spacecraft in all_replays.spacecraft.unique():
        date_str = datetime.now(UTC).strftime("%Y_%j")
        spacecraft_secrets = SpacecraftMapping.load("spacecraft-ids").mapping.get_secret_value()
        try:
            moc_index = spacecraft_secrets["moc"].index(df_spacecraft)
            soc_spacecraft_id = spacecraft_secrets["soc"][moc_index]
        except ValueError:  # we cannot find the spacecraft id and need to use an unknown indicator
            soc_spacecraft_id = 0
        file_spacecraft_id = {0: "UNKN", 1: "WFI01", 2: "WFI02", 3: "WFI03", 4: "NFI00"}[soc_spacecraft_id]
        df_path = os.path.join(pipeline_config['root'],
                               'REPLAY',
                               f'PUNCH_{file_spacecraft_id}_REPLAY_{date_str}.csv')
        new_entries = all_replays[all_replays.spacecraft == df_spacecraft]
        new_entries = new_entries.drop(columns=["spacecraft"])
        if os.path.exists(df_path):
            existing_table = pd.read_csv(df_path)
            new_table = pd.concat([existing_table, new_entries], ignore_index=True)
            new_table = new_table.drop_duplicates()
        else:
            new_table = new_entries
        os.makedirs(os.path.dirname(df_path), exist_ok=True)
        new_table.to_csv(df_path, index=False)

@flow
def level0_core_flow(pipeline_config: dict):
    logger = get_run_logger()
    engine, session = get_database_engine_session()

    tlm_xls_path = pipeline_config['tlm_xls_path']
    logger.info(f"Using {tlm_xls_path}")
    apids, tlm = read_tlm_defs(tlm_xls_path)
    database_classes = create_database_from_tlm(tlm, engine)
    apid_name2num = {row['Name']: int(row['APID'], base=16) for _, row in apids.iterrows()}
    defs = create_packet_definitions(tlm, parse_expanding_fields=False)

    new_tlm_files = detect_new_tlm_files(pipeline_config, session=session)
    logger.info(f"Found {len(new_tlm_files)} new TLM files")

    logger.debug("Proceeding through files")
    for i, path in enumerate(new_tlm_files):
        logger.info(f"{i+1}/{len(new_tlm_files)}: Processing {path}")
        ingest_tlm_file(path, session, defs, apid_name2num, database_classes)

    defs = create_packet_definitions(tlm, parse_expanding_fields=True)
    if new_tlm_files:
        level0_form_images(session, pipeline_config, database_classes, defs, apid_name2num)

@task
def level0_construct_flow_info(pipeline_config: dict):
    flow_type = "level0"
    state = "planned"
    creation_time = datetime.now(UTC)
    priority = pipeline_config["flows"][flow_type]["priority"]["initial"]

    call_data = json.dumps(
        {
            "pipeline_config": pipeline_config,
        }
    )
    return Flow(
        flow_type=flow_type,
        flow_level="0",
        state=state,
        creation_time=creation_time,
        priority=priority,
        call_data=call_data,
    )


@flow
def level0_scheduler_flow(pipeline_config_path=None, session=None, reference_time=None):
    pipeline_config = load_pipeline_configuration(pipeline_config_path)
    new_flow = level0_construct_flow_info(pipeline_config)

    if session is None:
        session = get_database_session()

    session.add(new_flow)
    session.commit()


@flow
def level0_process_flow(flow_id: int, pipeline_config_path=None , session=None):
    logger = get_run_logger()

    if session is None:
        session = get_database_session()

    # fetch the appropriate flow db entry
    flow_db_entry = session.query(Flow).where(Flow.flow_id == flow_id).one()
    logger.info(f"Running on flow db entry with id={flow_db_entry.flow_id}.")

    # update the processing flow name with the flow run name from Prefect
    flow_run_context = get_run_context()
    flow_db_entry.flow_run_name = flow_run_context.flow_run.name
    flow_db_entry.flow_run_id = flow_run_context.flow_run.id
    flow_db_entry.state = "running"
    flow_db_entry.start_time = datetime.now(UTC)
    session.commit()

    # load the call data and launch the core flow
    flow_call_data = json.loads(flow_db_entry.call_data)
    logger.info(f"Running with {flow_call_data}")
    try:
        level0_core_flow(**flow_call_data)
    except Exception as e:
        flow_db_entry.state = "failed"
        flow_db_entry.end_time = datetime.now(UTC)
        session.commit()
        raise e
    else:
        flow_db_entry.state = "completed"
        flow_db_entry.end_time = datetime.now(UTC)
        # Note: the file_db_entry gets updated above in the writing step because it could be created or blank
        session.commit()

if __name__ == "__main__":
    session = get_database_session()
    pipeline_config = load_pipeline_configuration("/Users/mhughes/repos/punchpipe/process_local_config.yaml")
    pipeline_config['plate_scale']['1'] = 0.02444444444
    pipeline_config['plate_scale']['2'] = 0.02444444444
    pipeline_config['plate_scale']['3'] = 0.02444444444
    pipeline_config['plate_scale']['4'] = 0.008333333333
    level0_core_flow(pipeline_config=pipeline_config)
