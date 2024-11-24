import os
from datetime import datetime

import numpy as np
import pandas as pd
from ndcube import NDCube
from prefect import flow
from prefect.blocks.system import Secret
from punchbowl.data import get_base_file_name
from punchbowl.data.io import write_ndcube_to_fits
from punchbowl.data.meta import NormalizedMetadata
from sqlalchemy import and_

from punchpipe import __version__ as software_version
from punchpipe.level0.core import detect_new_tlm_files, update_tlm_database, parse_new_tlm_files, process_telemetry_file, form_from_jpeg_compressed, image_is_okay, get_fits_metadata, form_preliminary_wcs
from punchpipe.level0.meta import POSITIONS_TO_CODES, convert_pfw_position_to_polarizer
from punchpipe.level0.ccsds import unpack_compression_settings
from punchpipe.control.util import get_database_session, load_pipeline_configuration
from punchpipe.control.db import TLMFiles, SciPacket, File

@flow
def level0_ingest_raw_packets(session=None):
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

@flow
def level0_form_images(session=None, pipeline_config_path="config.yaml"):
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
