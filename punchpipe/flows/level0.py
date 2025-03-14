import os
from datetime import datetime

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from ndcube import NDCube
from prefect import flow, get_run_logger
from prefect.blocks.core import Block
from prefect.blocks.fields import SecretDict
from punchbowl.data import get_base_file_name
from punchbowl.data.punch_io import write_ndcube_to_fits
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
from punchpipe.level0.meta import POSITIONS_TO_CODES, convert_pfw_position_to_polarizer, eci_quaternion_to_ra_dec


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
            sequence_counter = []
            for packet_entry in image_packets_entries:
                tlm_content_index = needed_tlm_paths.index(tlm_id_to_tlm_path[packet_entry.source_tlm_file])
                selected_tlm_contents = tlm_contents[tlm_content_index]
                ordered_image_content.append(selected_tlm_contents[0x20]['SCI_XFI_IMG_DATA'][packet_entry.packet_num])
                sequence_counter.append(selected_tlm_contents[0x20]['SCI_XFI_HDR_GRP'][packet_entry.packet_num])

            # Get the proper image
            skip_image = False
            sequence_counter_diff = np.diff(np.array(sequence_counter))
            if not np.all(np.isin(sequence_counter_diff, [1, 255])):
                skip_image = True
                error = {'start_time': image_packets_entries[0].timestamp.isoformat(),
                         'start_block': image_packets_entries[0].flash_block,
                         'replay_length': image_packets_entries[-1].flash_block
                                          - image_packets_entries[0].flash_block}
                errors.append(error)

            if image_compression[0]['CMP_BYP'] == 0 and image_compression[0]['JPEG'] == 1:  # this assumes the image compression is static for an image
                try:
                    ordered_image_content = np.concatenate(ordered_image_content)
                    image = form_from_jpeg_compressed(ordered_image_content)
                except (RuntimeError, ValueError):
                    skip_image = True
                    error = {'start_time': image_packets_entries[0].timestamp.isoformat(),
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
                    error = {'start_time': image_packets_entries[0].timestamp.isoformat(),
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


if __name__ == '__main__':
    from glob import glob
    import itertools

    tlm_paths = sorted(glob("/home/jmbhughes/data/raw_packets/*.tlm"))
    for i, tlm_path in enumerate(tlm_paths):
        print(i, tlm_path)

    i = 5

    print(i, tlm_paths[i])
    parsed_tlm = parse_new_tlm_files(tlm_paths[i])
    print(parsed_tlm[105].keys())

    packet_j = 730
    metadata = {
        'spacecraft_id': "1",
        'datetime': datetime.now(),
        'ATT_DET_Q_BODY_WRT_ECI1': parsed_tlm[105]['ATT_DET_Q_BODY_WRT_ECI1'][packet_j],
        'ATT_DET_Q_BODY_WRT_ECI2': parsed_tlm[105]['ATT_DET_Q_BODY_WRT_ECI2'][packet_j],
        'ATT_DET_Q_BODY_WRT_ECI3': parsed_tlm[105]['ATT_DET_Q_BODY_WRT_ECI3'][packet_j],
        'ATT_DET_Q_BODY_WRT_ECI4': parsed_tlm[105]['ATT_DET_Q_BODY_WRT_ECI4'][packet_j]}
    q = np.array([metadata['ATT_DET_Q_BODY_WRT_ECI2'] * 0.5E-10,
                           metadata['ATT_DET_Q_BODY_WRT_ECI1'] * 0.5E-10,
                           metadata['ATT_DET_Q_BODY_WRT_ECI3'] * 0.5E-10,
                           metadata['ATT_DET_Q_BODY_WRT_ECI4'] * 0.5E-10])
    w = form_preliminary_wcs(metadata, 0.0225)

    ras = []
    decs = []
    for p in itertools.permutations([0, 1, 2, 3]):
        p = np.array(p)
        print(p)
        print(np.array(q[p]))
        print(np.array(eci_quaternion_to_ra_dec(q[p])))
        print("-"*80)

    p = np.array([3, 1, 2, 0])
    all_ras = []
    all_decs = []
    for p in [[0, 1, 2, 3], [3, 1, 2, 0]]: #itertools.permutations([0, 1, 2, 3]):
        ras, decs = [], []
        for packet_j in range(len(parsed_tlm[105])):
            metadata = {
                'spacecraft_id': "1",
                'datetime': datetime.now(),
                'ATT_DET_Q_BODY_WRT_ECI1': parsed_tlm[105]['ATT_DET_Q_BODY_WRT_ECI1'][packet_j],
                'ATT_DET_Q_BODY_WRT_ECI2': parsed_tlm[105]['ATT_DET_Q_BODY_WRT_ECI2'][packet_j],
                'ATT_DET_Q_BODY_WRT_ECI3': parsed_tlm[105]['ATT_DET_Q_BODY_WRT_ECI3'][packet_j],
                'ATT_DET_Q_BODY_WRT_ECI4': parsed_tlm[105]['ATT_DET_Q_BODY_WRT_ECI4'][packet_j]}
            q = np.array([metadata['ATT_DET_Q_BODY_WRT_ECI2'] * 0.5E-10,
                          metadata['ATT_DET_Q_BODY_WRT_ECI1'] * 0.5E-10,
                          metadata['ATT_DET_Q_BODY_WRT_ECI3'] * 0.5E-10,
                          metadata['ATT_DET_Q_BODY_WRT_ECI4'] * 0.5E-10])[np.array(p)]
            cc = np.array(eci_quaternion_to_ra_dec(q))
            ras.append(360 - cc[0])
            decs.append(cc[1])
        all_ras.append(ras)
        all_decs.append(decs)


    fig, axs = plt.subplots(nrows=2)
    # axs[0].plot(all_ras[0], 'r')
    # axs[1].plot(all_decs[0], 'r')

    axs[0].plot(all_ras[1], 'b')
    axs[1].plot(all_decs[1], 'b')

    axs[0].set_ylabel("RA [deg]")
    axs[1].set_ylabel("Dec [deg]")
    axs[1].set_xlabel("Packet number")

    plt.show()
    # print(w)
