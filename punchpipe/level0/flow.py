import json
import base64
from datetime import datetime, timedelta
import warnings

import numpy as np
import sqlalchemy.exc
from prefect import flow, task
from sqlalchemy import and_
import pymysql
import pylibjpeg

from punchpipe.level0.ccsds import process_telemetry_file, PACKET_APID2NAME, unpack_compression_settings
from punchpipe.controlsegment.db import SciPacket, EngPacket
from punchpipe.controlsegment.util import (get_database_session)
from punchpipe.error import CCSDSPacketConstructionWarning, CCSDSPacketDatabaseUpdateWarning


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
def detect_new_tlm_files() -> [str]:
    # TODO: implement
    return ["/Users/jhughes/Desktop/data/PUNCH_CCSDS/RAW_CCSDS_DATA/PUNCH_NFI00_RAW_2024_160_19_37_V01.tlm"]


@task
def parse_new_tlm_files(telemetry_file_path: str):
    return process_telemetry_file(telemetry_file_path)


@task
def update_tlm_database(packets, telemetry_file_path: str, session=None):
    if session is None:
        session = get_database_session()

    for apid, this_apid_packets in packets.items():
        for i in range(len(this_apid_packets['CCSDS_APID'])):
            if apid in PACKET_APID2NAME:
                packet_name = PACKET_APID2NAME[apid]
                try:
                    seconds = int(this_apid_packets[packet_name + "_HDR_SEC"][i])
                    microseconds = int(this_apid_packets[packet_name + "_HDR_USEC"][i])
                except ValueError:
                    seconds = 0
                    microseconds = 0
                    warnings.warn("Time could not be properly extracted for packet.",
                                  CCSDSPacketConstructionWarning)
                timestamp = (datetime(2000, 1, 1)
                             + timedelta(seconds=seconds) + timedelta(microseconds=microseconds))

                try:
                    spacecraft_id = int(this_apid_packets[packet_name + "_HDR_SCID"][i])
                except ValueError:
                    spacecraft_id = -1
                    warnings.warn("Spacecraft ID could not be extracted for packet.",
                                  CCSDSPacketConstructionWarning)

                try:
                    flash_block_address = int(this_apid_packets[packet_name + "_HDR_FLASH_BLOCK"][i])
                except ValueError:
                    flash_block_address = -1
                    warnings.warn("Flash block address could not be extracted for packet.",
                                  CCSDSPacketConstructionWarning)

                try:
                    if "sci" in packet_name.lower():
                        this_packet = SciPacket(apid=apid,
                                             sequence_count=this_apid_packets['CCSDS_SEQUENCE_COUNT'][i],
                                             length=this_apid_packets['CCSDS_PACKET_LENGTH'][i],
                                             spacecraft_id=spacecraft_id,
                                             flash_block=flash_block_address,
                                             timestamp=timestamp,
                                             packet_num=i,
                                             source_tlm_file=telemetry_file_path,
                                             is_used=False,
                                             compression_settings=this_apid_packets['SCI_XFI_COM_SET'][i])
                    else:
                        this_packet = EngPacket(apid=apid,
                                             sequence_count=this_apid_packets['CCSDS_SEQUENCE_COUNT'][i],
                                             length=this_apid_packets['CCSDS_PACKET_LENGTH'][i],
                                             spacecraft_id=spacecraft_id,
                                             flash_block=flash_block_address,
                                             timestamp=timestamp,
                                             packet_num=i,
                                             source_tlm_file=telemetry_file_path,
                                             is_used=False)
                    session.add(this_packet)
                except (sqlalchemy.exc.DataError, pymysql.err.DataError) as e:
                    warnings.warn(f"Unable to add packet to database, {e}.", CCSDSPacketDatabaseUpdateWarning)
        session.commit()


@flow
def ingest_raw_packets():
    paths = detect_new_tlm_files()
    for path in paths:
        packets = parse_new_tlm_files(path)
        update_tlm_database(packets, path)


@flow
def form_level0_fits(session=None):
    if session is None:
        session = get_database_session()

    distinct_times = session.query(SciPacket.timestamp).distinct().all()
    distinct_spacecraft = session.query(SciPacket.spacecraft_id).distinct().all()
    print("distinct times", len(distinct_times))

    for spacecraft in distinct_spacecraft:
        for t in distinct_times:
            image_packets = session.query(SciPacket).where(and_(SciPacket.timestamp == t[0], SciPacket.spacecraft_id == spacecraft[0])).all()
            image_compression = [unpack_compression_settings(packet.compression_settings) for packet in image_packets]
            # TODO: open the packets again from the TLM files and get the data... avoid parsing the same TLM file multiple times
            # print("this image", len(image_packets), image_compression[0])
            if image_compression[0]['JPEG'] == 1:
                form_from_jpeg_compressed(image_packets)
            # TODO: do the square root decoding stuff
            # TODO: get all the metadata and put it in the right NormalizedMetadata
            # TODO: make a fits file
            # TODO: write fits file to disk

def form_from_jpeg_compressed(packets):
    # packets = sorted(packets, key=lambda packet: packet.timestamp)
    # reference_files = [packet.source_tlm_file for packet in packets]
    img = np.concatenate(packets[0x20]['SCI_XFI_IMG_DATA'][22:44])
    img = pylibjpeg.decode(img.tobytes())
    return img

if __name__ == '__main__':
    # ingest_raw_packets()
    # session = get_database_session()
    # results = session.query(SciPacket).where(SciPacket.is_used == False).all()
    # unique_times = list(set(r.timestamp for r in results))
    form_level0_fits()
