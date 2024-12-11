import os
from datetime import datetime

import numpy as np
import pandas as pd
from ndcube import NDCube
from prefect import flow, get_run_logger
from prefect.blocks.core import Block
from prefect.blocks.fields import SecretDict
from punchbowl.data import get_base_file_name
from punchbowl.data.io import write_ndcube_to_fits
from punchbowl.data.meta import NormalizedMetadata
from sqlalchemy import and_

from punchpipe import __version__ as software_version
from punchpipe.control.db import File, PacketHistory, SciPacket, TLMFiles
from punchpipe.control.util import get_database_session, load_pipeline_configuration
from punchpipe.level0.ccsds import unpack_compression_settings
from punchpipe.level0.core import (
    detect_new_tlm_files,
    form_from_jpeg_compressed,
    form_from_raw,
    form_preliminary_wcs,
    get_fits_metadata,
    parse_new_tlm_files,
    process_telemetry_file,
    update_tlm_database,
)
from punchpipe.level0.meta import POSITIONS_TO_CODES, convert_pfw_position_to_polarizer


class SpacecraftMapping(Block):
    mapping: SecretDict

@flow
def level0_ingest_raw_packets(pipeline_config_path: str | None = None, session=None):
    logger = get_run_logger()
    if session is None:
        session = get_database_session()
    config = load_pipeline_configuration(pipeline_config_path)
    logger.info(f"Querying {config['tlm_directory']}.")
    paths = detect_new_tlm_files(config, session=session)
    logger.info(f"Preparing to process {len(paths)} files.")

    # Commit to processing these TLM files
    tlm_files = []
    for path in paths:
        new_tlm_file = TLMFiles(path=path, is_processed=False)
        session.add(new_tlm_file)
        tlm_files.append(new_tlm_file)
    session.commit()

    # Actually performing processing
    for tlm_file in tlm_files:
        logger.info(f"Ingesting {tlm_file.path}.")
        try:
            packets = parse_new_tlm_files(tlm_file.path)
            tlm_file.is_processed = True
            session.commit()
            update_tlm_database(packets, tlm_file.tlm_id)
        except Exception as e:
            logger.error(f"Failed to ingest {tlm_file.path}: {e}")

@flow
def level0_form_images(session=None, pipeline_config_path=None):
    logger = get_run_logger()

    if session is None:
        session = get_database_session()

    config = load_pipeline_configuration(pipeline_config_path)

    distinct_times = session.query(SciPacket.timestamp).filter(~SciPacket.is_used).distinct().all()
    distinct_spacecraft = session.query(SciPacket.spacecraft_id).filter(~SciPacket.is_used).distinct().all()


    already_parsed_tlms = {} # tlm_path maps to the parsed contents

    skip_count, success_count = 0, 0
    for spacecraft in distinct_spacecraft:
        errors = []

        for t in distinct_times:
            image_packets_entries = (session.query(SciPacket)
                                     .where(and_(SciPacket.timestamp == t[0],
                                                                SciPacket.spacecraft_id == spacecraft[0])).all())
            image_compression = [unpack_compression_settings(packet.compression_settings)
                                 for packet in image_packets_entries]

            # Read all the relevant TLM files
            needed_tlm_ids = set([image_packet.source_tlm_file for image_packet in image_packets_entries])
            tlm_id_to_tlm_path = {tlm_id: session.query(TLMFiles.path).where(TLMFiles.tlm_id == tlm_id).one().path
                                  for tlm_id in needed_tlm_ids}
            needed_tlm_paths = list(session.query(TLMFiles.path).where(TLMFiles.tlm_id.in_(needed_tlm_ids)).all())
            needed_tlm_paths = [p.path for p in needed_tlm_paths]

            # parse any new TLM files needed
            for tlm_path in needed_tlm_paths:
                if tlm_path not in already_parsed_tlms:
                    already_parsed_tlms[tlm_path] = process_telemetry_file(tlm_path)

            # make it easy to grab the right TLM files
            tlm_contents = [already_parsed_tlms[tlm_path] for tlm_path in needed_tlm_paths]

            # Form the image packet stream for decompression
            ordered_image_content = []
            for packet_entry in image_packets_entries:
                tlm_content_index = needed_tlm_paths.index(tlm_id_to_tlm_path[packet_entry.source_tlm_file])
                selected_tlm_contents = tlm_contents[tlm_content_index]
                ordered_image_content.append(selected_tlm_contents[0x20]['SCI_XFI_IMG_DATA'][packet_entry.packet_num])

            # Get the proper image
            skip_image = False
            if image_compression[0]['CMP_BYP'] == 0 and image_compression[0]['JPEG'] == 1:  # this assumes the image compression is static for an image
                try:
                    ordered_image_content = np.concatenate(ordered_image_content)
                    image = form_from_jpeg_compressed(ordered_image_content)
                except (RuntimeError, ValueError):
                    skip_image = True
                    error = {'start_time': image_packets_entries[0].timestamp.strftime("%Y-%m-%d %h:%m:%s"),
                             'start_block': image_packets_entries[0].flash_block,
                             'replay_length': image_packets_entries[-1].flash_block
                                              - image_packets_entries[0].flash_block}
                    errors.append(error)
            elif image_compression[0]['CMP_BYP'] == 1:
                try:
                    ordered_image_content = np.concatenate(ordered_image_content)
                    logger.info(f"Packet shape {ordered_image_content.shape[0]}", )
                    image = form_from_raw(ordered_image_content)
                except (RuntimeError, ValueError):
                    skip_image = True
                    error = {'start_time': image_packets_entries[0].timestamp.strftime("%Y-%m-%d %h:%m:%s"),
                             'start_block': image_packets_entries[0].flash_block,
                             'replay_length': image_packets_entries[-1].flash_block
                                              - image_packets_entries[0].flash_block}
                    errors.append(error)
            else:
                skip_image = True
                print("Not implemented")

            if not skip_image:
                spacecraft_secrets = SpacecraftMapping.load("spacecraft-ids").mapping.get_secret_value()
                moc_index = spacecraft_secrets["moc"].index(image_packets_entries[0].spacecraft_id)
                spacecraft_id = spacecraft_secrets["soc"][moc_index]

                metadata_contents = get_fits_metadata(image_packets_entries[0].timestamp,
                                                      image_packets_entries[0].spacecraft_id,
                                                      session)
                file_type = POSITIONS_TO_CODES[convert_pfw_position_to_polarizer(metadata_contents['POSITION_CURR'])]
                preliminary_wcs = form_preliminary_wcs(metadata_contents, float(config['plate_scale'][spacecraft_id]))
                meta = NormalizedMetadata.load_template(file_type + str(spacecraft_id), "0")
                # TODO : activate later
                # for meta_key, meta_value in metadata_contents.items():
                #     meta[meta_key] = meta_value
                meta['DATE-OBS'] = str(t[0])
                cube = NDCube(data=image, meta=meta, wcs=preliminary_wcs)

                l0_db_entry = File(level="0",
                                   file_type=file_type,
                                   observatory=str(spacecraft_id),
                                   file_version="1",  # TODO: increment the file version
                                   software_version=software_version,
                                   date_created=datetime.now(),
                                   date_obs=t[0],
                                   date_beg=t[0],
                                   date_end=t[0],
                                   state="created")

                out_path =  os.path.join(l0_db_entry.directory(config['root']), get_base_file_name(cube)) + ".fits"
                os.makedirs(os.path.dirname(out_path), exist_ok=True)
                write_ndcube_to_fits(cube,out_path)
                # TODO: write a jp2
                for image_packets_entries in image_packets_entries:
                    image_packets_entries.is_used = True
                session.add(l0_db_entry)
                session.commit()
                success_count += 1
            else:
                skip_count += 1
        history = PacketHistory(datetime=datetime.now(),
                               num_images_succeeded=success_count,
                               num_images_failed=skip_count)
        session.add(history)
        session.commit()

        df_errors = pd.DataFrame(errors)
        date_str = datetime.now().strftime("%Y_%j")
        df_path = os.path.join(config['root'], 'REPLAY', f'PUNCH_{str(spacecraft[0])}_REPLAY_{date_str}.csv')
        os.makedirs(os.path.dirname(df_path), exist_ok=True)
        df_errors.to_csv(df_path, index=False)
