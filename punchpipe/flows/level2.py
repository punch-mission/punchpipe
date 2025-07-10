import os
import json
import typing as t
from datetime import UTC, datetime, timedelta

from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from punchbowl.level2.flow import level2_core_flow
from punchbowl.util import average_datetime

from punchpipe import __version__
from punchpipe.control.db import File, Flow
from punchpipe.control.processor import generic_process_flow_logic
from punchpipe.control.scheduler import generic_scheduler_flow_logic

SCIENCE_POLARIZED_LEVEL1_TYPES = ["PM", "PZ", "PP"]
SCIENCE_CLEAR_LEVEL1_TYPES = ["CR"]


@task(cache_policy=NO_CACHE)
def level2_query_ready_files(session, pipeline_config: dict, reference_time=None, max_n=9e99):
    return _level2_query_ready_files(session, polarized=True, pipeline_config=pipeline_config, max_n=max_n)


@task(cache_policy=NO_CACHE)
def level2_query_ready_clear_files(session, pipeline_config: dict, reference_time=None, max_n=9e99):
    return _level2_query_ready_files(session, polarized=False, pipeline_config=pipeline_config, max_n=max_n)


def _level2_query_ready_files(session, polarized: bool, pipeline_config: dict, max_n=9e99):
    logger = get_run_logger()
    all_ready_files = (session.query(File).filter(File.state == "quickpunched")
                       .filter(File.level == "1")
                        # TODO: This line temporarily excludes NFI
                       .filter(File.observatory.in_(['1', '2', '3']))
                       .filter(File.file_type.in_(
                            SCIENCE_POLARIZED_LEVEL1_TYPES if polarized else SCIENCE_CLEAR_LEVEL1_TYPES))
                       .order_by(File.date_obs.asc()).all())
    logger.info(f"{len(all_ready_files)} ready files")

    if len(all_ready_files) == 0:
        return []

    # We need to group up files by date_obs, but we need to handle small variations in date_obs. The files are coming
    # from the database already sorted, so let's just walk through the list of files and cut a group boundary every time
    # date_obs increases by more than a threshold.
    grouped_files = []
    # We'll keep track of where the current group started, and then keep stepping to find the end of this group.
    group_start = 0
    tstamp_start = all_ready_files[0].date_obs.replace(tzinfo=UTC).timestamp()
    file_under_consideration = 0
    while True:
        file_under_consideration += 1
        if file_under_consideration == len(all_ready_files):
            break
        this_tstamp = all_ready_files[file_under_consideration].date_obs.replace(tzinfo=UTC).timestamp()
        if abs(this_tstamp - tstamp_start) > 10:
            # date_obs has jumped by more than our tolerance, so let's cut the group and then start tracking the next
            # one
            grouped_files.append(all_ready_files[group_start:file_under_consideration])
            group_start = file_under_consideration
            tstamp_start = this_tstamp
    grouped_files.append(all_ready_files[group_start:])

    logger.info(f"{len(grouped_files)} unique times")
    grouped_ready_files = []
    cutoff_time = (pipeline_config["flows"]["level2" if polarized else "level2_clear"]
                   .get("ignore_missing_after_days", None))
    if cutoff_time is not None:
        cutoff_time = datetime.now(tz=UTC) - timedelta(days=cutoff_time)
    for group in grouped_files:
        # TODO: This line temporarily excludes NFI
        # if (len(group) == (12 if polarized else 4)
        if (len(group) == (9 if polarized else 3)
                or (cutoff_time and group[-1].date_obs.replace(tzinfo=UTC) < cutoff_time)):
            grouped_ready_files.append([f.file_id for f in group])
        if len(grouped_ready_files) >= max_n:
            break
    logger.info(f"{len(grouped_ready_files)} groups heading out")
    return grouped_ready_files


@task(cache_policy=NO_CACHE)
def level2_construct_flow_info(level1_files: list[File], level2_file: File, pipeline_config: dict, session=None, reference_time=None):
    flow_type = "level2_clear" if level1_files[0].file_type == "CR" else "level2"
    state = "planned"
    creation_time = datetime.now()
    priority = pipeline_config["flows"][flow_type]["priority"]["initial"]
    call_data = json.dumps(
        {
            "data_list": [
                os.path.join(level1_file.directory(pipeline_config["root"]), level1_file.filename())
                for level1_file in level1_files
            ],
            "voter_filenames": [[] for _ in level1_files],
        }
    )
    return Flow(
        flow_type=flow_type,
        state=state,
        flow_level="2",
        creation_time=creation_time,
        priority=priority,
        call_data=call_data,
    )


@task
def level2_construct_file_info(level1_files: t.List[File], pipeline_config: dict, reference_time=None) -> t.List[File]:
    return [File(
                level="2",
                file_type="CT" if level1_files[0].file_type == "CR" else "PT",
                observatory="M",
                file_version=pipeline_config["file_version"],
                software_version=__version__,
                date_obs=average_datetime([f.date_obs for f in level1_files]),
                state="planned",
            )]


@flow
def level2_scheduler_flow(pipeline_config_path=None, session=None, reference_time=None):
    generic_scheduler_flow_logic(
        level2_query_ready_files,
        level2_construct_file_info,
        level2_construct_flow_info,
        pipeline_config_path,
        reference_time=reference_time,
        session=session,
    )


@flow
def level2_clear_scheduler_flow(pipeline_config_path=None, session=None, reference_time=None):
    generic_scheduler_flow_logic(
        level2_query_ready_clear_files,
        level2_construct_file_info,
        level2_construct_flow_info,
        pipeline_config_path,
        reference_time=reference_time,
        session=session,
    )

@flow
def level2_process_flow(flow_id: int, pipeline_config_path=None, session=None):
    generic_process_flow_logic(flow_id, level2_core_flow, pipeline_config_path, session=session)


@flow
def level2_clear_process_flow(flow_id: int, pipeline_config_path=None, session=None):
    generic_process_flow_logic(flow_id, level2_core_flow, pipeline_config_path, session=session)
