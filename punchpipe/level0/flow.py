import os
import json
import base64
import warnings
import importlib.metadata
from glob import glob
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pylibjpeg
import pymysql
import sqlalchemy.exc
from astropy.wcs import WCS
from ndcube import NDCube
from prefect import flow, task
from prefect.blocks.system import Secret
from punchbowl.data import get_base_file_name
from punchbowl.data.io import write_ndcube_to_fits
from punchbowl.data.meta import NormalizedMetadata
from punchbowl.data.wcs import calculate_helio_wcs_from_celestial, calculate_pc_matrix
from sqlalchemy import and_
from sunpy.coordinates import sun

from punchpipe.controlsegment.db import ENGPFWPacket, EngXACTPacket, File, SciPacket, TLMFiles, get_closest_eng_packets
from punchpipe.controlsegment.util import get_database_session, load_pipeline_configuration
from punchpipe.error import CCSDSPacketConstructionWarning, CCSDSPacketDatabaseUpdateWarning
from punchpipe.level0.ccsds import PACKET_APID2NAME, process_telemetry_file, unpack_compression_settings
from punchpipe.level0.meta import POSITIONS_TO_CODES, convert_pfw_position_to_polarizer, eci_quaternion_to_ra_dec

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
def detect_new_tlm_files(session=None) -> [str]:
    if session is None:
        session = get_database_session()

    tlm_directory = load_pipeline_configuration()['tlm_directory']
    found_tlm_files = set(glob(tlm_directory + '/*.tlm'))
    database_tlm_files = set(session.query(TLMFiles.path).distinct().all())
    return list(found_tlm_files - database_tlm_files)


@task
def parse_new_tlm_files(telemetry_file_path: str):
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
                             compression_settings=packet['SCI_XFI_COM_SET'])
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
                HOME_POSITION_OVRD=packet['HOME_POSITION_OVRD'],
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
@task
def update_tlm_database(packets, telemetry_file_path: str, session=None):
    if session is None:
        session = get_database_session()

    for apid, this_apid_packets in packets.items():
        for i in range(len(this_apid_packets['CCSDS_APID'])):
            if apid in PACKET_APID2NAME:
                try:
                    this_packet = form_packet_entry(apid, this_apid_packets[i], i, telemetry_file_path)
                    session.add(this_packet)
                except (sqlalchemy.exc.DataError, pymysql.err.DataError) as e:
                    warnings.warn(f"Unable to add packet to database, {e}.", CCSDSPacketDatabaseUpdateWarning)
        session.commit()


@flow
def ingest_raw_packets(session=None):
    if session is None:
        session = get_database_session()

    paths = detect_new_tlm_files(session=session)
    for path in paths:
        packets = parse_new_tlm_files(path)
        update_tlm_database(packets, path)

        # update the database with this tlm file
        new_tlm_file = TLMFiles(path=path, is_processed=True)
        session.add(new_tlm_file)
        session.commit()

def interpolate_value(query_time, before_time, before_value, after_time, after_value):
    if query_time == before_time:
        return before_value
    elif query_time == after_time:
        return after_value
    else:
        return ((after_value - before_value)
         * (query_time - before_time) / (after_time - before_value)
         + before_value)

def get_fits_metadata(observation_time, spacecraft_id):
    before_xact, after_xact = get_closest_eng_packets(EngXACTPacket, observation_time, spacecraft_id)
    ATT_DET_Q_BODY_WRT_ECI1 = interpolate_value(observation_time,
                                                before_xact.timestamp, before_xact['ATT_DET_Q_BODY_WRT_ECI1'],
                                                after_xact.timestam, after_xact['ATT_DET_Q_BODY_WRT_ECI1'])
    ATT_DET_Q_BODY_WRT_ECI2 = interpolate_value(observation_time,
                                                before_xact.timestamp, before_xact['ATT_DET_Q_BODY_WRT_ECI2'],
                                                after_xact.timestam, after_xact['ATT_DET_Q_BODY_WRT_ECI2'])
    ATT_DET_Q_BODY_WRT_ECI3 = interpolate_value(observation_time,
                                                before_xact.timestamp, before_xact['ATT_DET_Q_BODY_WRT_ECI3'],
                                                after_xact.timestam, after_xact['ATT_DET_Q_BODY_WRT_ECI3'])
    ATT_DET_Q_BODY_WRT_ECI4 = interpolate_value(observation_time,
                                                before_xact.timestamp, before_xact['ATT_DET_Q_BODY_WRT_ECI4'],
                                                after_xact.timestam, after_xact['ATT_DET_Q_BODY_WRT_ECI4'])

    before_pfw, _ = get_closest_eng_packets(ENGPFWPacket, observation_time, spacecraft_id)
    return {'spacecraft_id': spacecraft_id,
            'datetime': observation_time,
            'ATT_DET_Q_BODY_WRT_ECI1': ATT_DET_Q_BODY_WRT_ECI1,
            'ATT_DET_Q_BODY_WRT_ECI2': ATT_DET_Q_BODY_WRT_ECI2,
            'ATT_DET_Q_BODY_WRT_ECI3': ATT_DET_Q_BODY_WRT_ECI3,
            'ATT_DET_Q_BODY_WRT_ECI4': ATT_DET_Q_BODY_WRT_ECI4,
            'POSITION_CURR': before_pfw['POSITION_CURR']}


def form_preliminary_wcs(metadata, plate_scale):
    """Create the preliminary WCS for punchbowl"""
    quaternion = np.array([metadata['ATT_DET_Q_BODY_WRT_ECI1'],
                           metadata['ATT_DET_Q_BODY_WRT_ECI2'],
                           metadata['ATT_DET_Q_BODY_WRT_ECI3'],
                           metadata['ATT_DET_Q_BODY_WRT_ECI4']])
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

    return calculate_helio_wcs_from_celestial(celestial_wcs, metadata['datetime'], (2048, 2048))

def image_is_okay(image, pipeline_config):
    """Check that the formed image conforms to image quality expectations"""
    return pipeline_config['quality_check']['mean_low'] < np.mean(image) < pipeline_config['quality_check']['mean_high']

def form_from_jpeg_compressed(packets):
    """Form a JPEG-LS image from packets"""
    img = pylibjpeg.decode(packets.tobytes())
    return img

@flow
def form_level0_fits(session=None, pipeline_config_path="config.yaml"):
    if session is None:
        session = get_database_session()

    config = load_pipeline_configuration(pipeline_config_path)

    distinct_times = session.query(SciPacket.timestamp).filter(~SciPacket.is_used).distinct().all()
    distinct_spacecraft = session.query(SciPacket.spacecraft_id).filter(~SciPacket.is_used).distinct().all()


    for spacecraft in distinct_spacecraft:
        errors = []

        for t in distinct_times:
            image_packets_entries = session.query(SciPacket).where(and_(SciPacket.timestamp == t[0],
                                                                SciPacket.spacecraft_id == spacecraft[0])).all()
            image_compression = [unpack_compression_settings(packet.compression_settings)
                                 for packet in image_packets_entries]

            # Read all the relevant TLM files
            needed_tlm_ids = set([image_packet.source_tlm_file for image_packet in image_packets_entries])
            tlm_id_to_tlm_path = {tlm_id: session.query(TLMFiles.path).where(TLMFiles.tlm_id == tlm_id)
                                  for tlm_id in needed_tlm_ids}
            needed_tlm_paths = list(session.query(TLMFiles.path).where(TLMFiles.tlm_id.in_(needed_tlm_ids)).all())
            tlm_contents = [process_telemetry_file(tlm_path) for tlm_path in needed_tlm_paths]

            # Form the image packet stream for decompression
            ordered_image_content = []
            for packet_entry in image_packets_entries:
                tlm_content_index = needed_tlm_paths.index(tlm_id_to_tlm_path[packet_entry.source_tlm_file])
                selected_tlm_contents = tlm_contents[tlm_content_index]
                ordered_image_content.append(selected_tlm_contents[0x20]['SCI_XFI_IMG_DATA'][packet_entry.packet_num])
            ordered_image_content = np.concatenate(ordered_image_content)

            # Get the proper image
            skip_image = False
            if image_compression[0]['JPEG'] == 1:  # this assumes the image compression is static for an image
                try:
                    image = form_from_jpeg_compressed(ordered_image_content)
                except ValueError:
                    error = {'start_time': image_packets_entries[0].timestamp.strftime("%Y-%m-%d %h:%m:%s"),
                             'start_block': image_packets_entries[0].flash_block,
                             'replay_length': image_packets_entries[-1].flash_block
                                              - image_packets_entries[0].flash_block}
                    errors.append(error)
                else:
                    skip_image = True
            else:
                skip_image = True
                error = {'start_time': image_packets_entries[0].timestamp.strftime("%Y-%m-%d %h:%m:%s"),
                         'start_block': image_packets_entries[0].flash_block,
                         'replay_length': image_packets_entries[-1].flash_block
                                          - image_packets_entries[0].flash_block}
                errors.append(error)

            # check the quality of the image
            if not skip_image and not image_is_okay(image, config):
                skip_image = True
                error = {'start_time': image_packets_entries[0].timestamp.strftime("%Y-%m-%d %h:%m:%s"),
                         'start_block': image_packets_entries[0].flash_block,
                         'replay_length': image_packets_entries[-1].flash_block
                                          - image_packets_entries[0].flash_block}
                errors.append(error)

            if not skip_image:
                spacecraft_secrets = Secret.load("spacecraft-ids")
                spacecraft_id_mapper = spacecraft_secrets.get()
                spacecraft_id = spacecraft_id_mapper[image_packets_entries[0].spacecraft_id]

                metadata_contents = get_fits_metadata(image_packets_entries[0].timestamp, spacecraft_id)
                file_type = POSITIONS_TO_CODES[convert_pfw_position_to_polarizer(metadata_contents['POSITION_CURR'])]
                preliminary_wcs = form_preliminary_wcs(metadata_contents, config['plate_scale'][spacecraft_id])
                meta = NormalizedMetadata.load_template(file_type + spacecraft_id, "0")
                for meta_key, meta_value in metadata_contents.items():
                    meta[meta_key] = meta_value
                cube = NDCube(data=image, metadata=meta, wcs=preliminary_wcs)

                l0_db_entry = File(level="0",
                                   file_type=file_type,
                                   observatory=str(spacecraft_id),
                                   file_version="1",  # TODO: increment the file version
                                   software_version=software_version,
                                   date_created=datetime.now(),
                                   date_obs=t,
                                   date_beg=t,
                                   date_end=t,
                                   state="created")

                write_ndcube_to_fits(cube, os.path.join(l0_db_entry.directory(config['data_path']),
                                                        get_base_file_name(cube)))
                # TODO: write a jp2
                for image_packets_entries in image_packets_entries:
                    image_packets_entries.is_used = True
                session.add(l0_db_entry)
                session.commit()
        df_errors = pd.DataFrame(errors)
        date_str = datetime.now().strftime("%Y_%j")
        df_path = os.path.join(config['root'], 'REPLAY', f'PUNCH_{spacecraft}_REPLAY_{date_str}.csv')
        os.makedirs(df_path, exist_ok=True)
        df_errors.to_csv(df_path, index=False)
