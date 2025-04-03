import os
import json
import traceback
from datetime import UTC, datetime

import numpy as np
import pandas as pd
from ndcube import NDCube
from prefect import flow, get_run_logger, task
from prefect.blocks.core import Block
from prefect.blocks.fields import SecretDict
from prefect.context import get_run_context
from punchbowl.data import get_base_file_name
from punchbowl.data.meta import NormalizedMetadata
from punchbowl.data.punch_io import write_ndcube_to_fits
from sqlalchemy import and_

from punchpipe import __version__ as software_version
from punchpipe.control.db import File, Flow, PacketHistory, SciPacket, TLMFiles
from punchpipe.control.util import get_database_session, load_pipeline_configuration
from punchpipe.level0.ccsds import unpack_acquisition_settings, unpack_compression_settings
from punchpipe.level0.core import (
    detect_new_tlm_files,
    form_from_jpeg_compressed,
    form_from_raw,
    form_preliminary_wcs,
    get_metadata,
    parse_new_tlm_files,
    process_telemetry_file,
    update_tlm_database,
)


class SpacecraftMapping(Block):
    mapping: SecretDict

@flow
def level0_ingest_raw_packets(pipeline_config: str | dict | None = None, session=None):
    logger = get_run_logger()
    if session is None:
        session = get_database_session()
    if isinstance(pipeline_config, str):
        config = load_pipeline_configuration(pipeline_config)
    elif isinstance(pipeline_config, dict):
        config = pipeline_config
    else:
        raise RuntimeError("Empty pipeline config.")
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
    for i, tlm_file in enumerate(tlm_files):
        logger.info(f"{i}/{len(tlm_files)}: Ingesting {tlm_file.path}.")
        try:
            packets = parse_new_tlm_files(tlm_file.path)
            tlm_file.is_processed = True
            session.commit()
            update_tlm_database(packets, tlm_file.tlm_id)
        except Exception as e:
            logger.error(f"Failed to ingest {tlm_file.path}: {e}")

@flow
def level0_form_images(session=None, pipeline_config: str | dict | None = None):
    logger = get_run_logger()

    if session is None:
        session = get_database_session()

    if isinstance(pipeline_config, str):
        config = load_pipeline_configuration(pipeline_config)
    elif isinstance(pipeline_config, dict):
        config = pipeline_config
    else:
        raise RuntimeError("Empty pipeline config.")

    distinct_spacecraft = session.query(SciPacket.spacecraft_id).filter(~SciPacket.is_used).distinct().all()

    already_parsed_tlms = {} # tlm_path maps to the parsed contents

    skip_count, success_count = 0, 0
    for spacecraft in distinct_spacecraft:
        errors = []
        distinct_times = (session.query(SciPacket.timestamp)
                          .filter(~SciPacket.is_used)
                          .filter(SciPacket.spacecraft_id == spacecraft[0])
                          .distinct()
                          .all())

        for t in distinct_times:
            logger.info(f"Processing spacecraft={spacecraft[0]} at time={t[0]}")
            image_packets_entries = (session.query(SciPacket)
                                     .where(and_(SciPacket.timestamp == t[0],
                                                                SciPacket.spacecraft_id == spacecraft[0])).all())
            logger.info(f"len(packets) = {len(image_packets_entries)}")
            image_compression = [unpack_compression_settings(packet.compression_settings)
                                 for packet in image_packets_entries]
            logger.info(f"image_compression = {image_compression[0]}")

            # Read all the relevant TLM files
            needed_tlm_ids = set([image_packet.source_tlm_file for image_packet in image_packets_entries])
            tlm_id_to_tlm_path = {tlm_id: session.query(TLMFiles.path).where(TLMFiles.tlm_id == tlm_id).one().path
                                  for tlm_id in needed_tlm_ids}
            needed_tlm_paths = list(session.query(TLMFiles.path).where(TLMFiles.tlm_id.in_(needed_tlm_ids)).all())
            needed_tlm_paths = [p.path for p in needed_tlm_paths]
            logger.info(f"Will use data from {needed_tlm_paths}")

            # parse any new TLM files needed
            for tlm_path in needed_tlm_paths:
                if tlm_path not in already_parsed_tlms:
                    logger.info(f"Loading {tlm_path}...")
                    already_parsed_tlms[tlm_path] = process_telemetry_file(tlm_path)

            # make it easy to grab the right TLM files
            tlm_contents = [already_parsed_tlms[tlm_path] for tlm_path in needed_tlm_paths]

            # order the packets in the correct order for de-commutation
            order_dict = {}
            packet_entry_mapping = {}
            for packet_entry in image_packets_entries:
                packet_num = packet_entry.packet_num
                if packet_num in order_dict:
                    order_dict[packet_num].append(packet_entry.packet_id)
                else:
                    order_dict[packet_num] = [packet_entry.packet_id]
                packet_entry_mapping[packet_entry.packet_id] = packet_entry

            ordered_image_content = []
            sequence_counter = []
            for packet_num in sorted(list(order_dict.keys())):
                best_packet = max(order_dict[packet_num])
                packet_entry = packet_entry_mapping[best_packet]
                tlm_content_index = needed_tlm_paths.index(tlm_id_to_tlm_path[packet_entry.source_tlm_file])
                selected_tlm_contents = tlm_contents[tlm_content_index]
                ordered_image_content.append(selected_tlm_contents[0x20]['SCI_XFI_IMG_DATA'][packet_entry.packet_num])
                sequence_counter.append(selected_tlm_contents[0x20]['SCI_XFI_HDR_GRP'][packet_entry.packet_num])

            skip_image = False

            if image_compression[0]['CMP_BYP'] == 0 and image_compression[0]['JPEG'] == 1:  # this assumes the image compression is static for an image
                try:
                    ordered_image_content = np.concatenate(ordered_image_content)
                    image = form_from_jpeg_compressed(ordered_image_content)
                except (RuntimeError, ValueError):
                    logger.error("Could not form image")
                    skip_image = True
                    error = {'start_time': image_packets_entries[0].timestamp.isoformat(),
                             'start_block': image_packets_entries[0].flash_block,
                             'replay_length': image_packets_entries[-1].flash_block
                                              - image_packets_entries[0].flash_block + 1}
                    errors.append(error)
            else:
                try:
                    ordered_image_content = np.concatenate(ordered_image_content)
                    logger.info(f"Packet shape {ordered_image_content.shape[0]}", )
                    image = form_from_raw(ordered_image_content)
                except (RuntimeError, ValueError):
                    logger.error("Could not form image")
                    skip_image = True
                    error = {'start_time': image_packets_entries[0].timestamp.isoformat(),
                             'start_block': image_packets_entries[0].flash_block,
                             'replay_length': image_packets_entries[-1].flash_block
                                              - image_packets_entries[0].flash_block + 1}
                    errors.append(error)

            if not skip_image:
                try:
                    spacecraft_secrets = SpacecraftMapping.load("spacecraft-ids").mapping.get_secret_value()
                    moc_index = spacecraft_secrets["moc"].index(image_packets_entries[0].spacecraft_id)
                    spacecraft_id = spacecraft_secrets["soc"][moc_index]
                    acquisition_settings = unpack_acquisition_settings(image_packets_entries[0].acquisition_settings)
                    position_info, fits_info = get_metadata(image_packets_entries[0].timestamp,
                                                     image_packets_entries[0].spacecraft_id,
                                                     image.shape,
                                                     acquisition_settings['EXPOSURE'] / 10,  # exptime in seconds
                                                     session)
                    file_type = fits_info["TYPECODE"]
                    preliminary_wcs = form_preliminary_wcs(position_info, float(config['plate_scale'][str(spacecraft_id)]))
                    meta = NormalizedMetadata.load_template(file_type + str(spacecraft_id), "0")

                    for meta_key, meta_value in fits_info.items():
                        meta[meta_key] = meta_value
                    meta['DATE-OBS'] = str(t[0])
                    meta['EXPTIME'] = acquisition_settings['EXPOSURE'] / 10.0

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

                    logger.info(f"Writing to {out_path}")
                    write_ndcube_to_fits(cube, out_path)
                    # TODO: write a jp2
                    for image_packets_entries in image_packets_entries:
                        image_packets_entries.is_used = True
                    session.add(l0_db_entry)
                    session.commit()
                    success_count += 1
                except Exception as e:
                    logger.error(f"Failed writing image because: {e}")
                    logger.error(traceback.format_exc())
                    skip_count += 1
            else:
                skip_count += 1
        history = PacketHistory(datetime=datetime.now(UTC),
                               num_images_succeeded=success_count,
                               num_images_failed=skip_count)
        session.add(history)
        session.commit()

        # TODO: split into multiple files and append updates instead of making a new file each time
        df_errors = pd.DataFrame(errors)
        date_str = datetime.now(UTC).strftime("%Y_%j")
        df_path = os.path.join(config['root'], 'REPLAY', f'PUNCH_{str(spacecraft[0])}_REPLAY_{date_str}.csv')
        os.makedirs(os.path.dirname(df_path), exist_ok=True)
        df_errors.to_csv(df_path, index=False)


@task
def level0_construct_flow_info(pipeline_config: dict):
    flow_type = "level0"
    state = "planned"
    creation_time = datetime.now()
    priority = pipeline_config["flows"][flow_type]["priority"]["initial"]

    call_data = json.dumps(
        {
            "pipeline_config": pipeline_config,
            "session": None
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
def level0_core_flow(pipeline_config: str | dict | None = None, session=None):
    level0_ingest_raw_packets(pipeline_config=pipeline_config, session=session)
    level0_form_images(pipeline_config=pipeline_config, session=session)


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
    flow_db_entry.start_time = datetime.now()
    session.commit()

    # load the call data and launch the core flow
    flow_call_data = json.loads(flow_db_entry.call_data)
    logger.info(f"Running with {flow_call_data}")
    try:
        level0_core_flow(**flow_call_data)
    except Exception as e:
        flow_db_entry.state = "failed"
        flow_db_entry.end_time = datetime.now()
        session.commit()
        raise e
    else:
        flow_db_entry.state = "completed"
        flow_db_entry.end_time = datetime.now()
        # Note: the file_db_entry gets updated above in the writing step because it could be created or blank
        session.commit()

if __name__ == "__main__":
    session = get_database_session()
    pipeline_config = load_pipeline_configuration("/Users/mhughes/repos/punchpipe/process_local_config.yaml")
    pipeline_config['plate_scale']['1'] = 0.02444444444
    pipeline_config['plate_scale']['2'] = 0.02444444444
    pipeline_config['plate_scale']['3'] = 0.02444444444
    pipeline_config['plate_scale']['4'] = 0.008333333333
    # level0_ingest_raw_packets(pipeline_config=pipeline_config, session=session)
    level0_form_images(pipeline_config=pipeline_config, session=session)
