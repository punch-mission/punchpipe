import multiprocessing
import argparse
import os
import traceback
from collections import defaultdict
from datetime import datetime
import yaml
from yaml.loader import FullLoader
import warnings
import time

from tqdm.auto import tqdm
from prefect.logging import disable_run_logger
from sqlalchemy import update

from punchpipe.control.db import Flow
from punchpipe.cli import find_flow
from punchpipe.control.util import get_database_session


def load_pipeline_configuration(path: str = None) -> dict:
    with open(path) as f:
        config = yaml.load(f, Loader=FullLoader)
    # TODO: add validation
    return config


def load_enabled_flows(pipeline_config):
    enabled_flows = []
    for flow_type in pipeline_config["flows"]:
        if pipeline_config["flows"][flow_type].get("enabled", True) == "speedy":
            enabled_flows.append(flow_type)
    return enabled_flows


def gather_planned_flows(session, enabled_flows, max_n=None):
    flows = (session.query(Flow)
             .where(Flow.state == "planned")
             .where(Flow.flow_type.in_(enabled_flows))
             .order_by(Flow.is_backprocessing.asc(), Flow.priority.desc(), Flow.creation_time.desc())
             .limit(max_n).all())
    count_per_type = defaultdict(lambda: 0)
    flow_ids = []
    types = []
    for flow in flows:
        types.append(flow.flow_type)
        count_per_type[flow.flow_type] += 1
        flow_ids.append(flow.flow_id)

    return flow_ids, types, count_per_type


def worker_init(config_path):
    global session, flow_type_to_runner, path_to_config
    with disable_run_logger(), warnings.catch_warnings():
        # Otherwise warning spam will hide any progress messages
        warnings.simplefilter('ignore')
        session = get_database_session()
    flow_type_to_runner = dict()
    path_to_config = config_path


def worker_run_flow(inputs):
    flow_id, flow_type = inputs
    global flow_type_to_runner, session, path_to_config
    if flow_type not in flow_type_to_runner:
        runner = find_flow(flow_type + "_process_flow").fn
        flow_type_to_runner[flow_type] = runner
    else:
        runner = flow_type_to_runner[flow_type]

    session.execute(update(Flow).where(Flow.flow_id == flow_id).values(
            state='launched', flow_run_name='speedster', launch_time=datetime.now()))

    with disable_run_logger(), warnings.catch_warnings():
        # Otherwise warning spam will hide any progress messages
        warnings.simplefilter('ignore')
        try:
            runner(flow_id, path_to_config, session)
        except KeyboardInterrupt:
            session.execute(
                update(Flow).where(Flow.flow_id == flow_id).values(state='revivable'))
            session.commit()
            print(f"Keyboard interrupt in flow {flow_id}; marked as revivable")
        except:
            print(f"Exception in flow {flow_id}")
            traceback.print_exc()


if __name__ == "__main__":
    multiprocessing.set_start_method('forkserver')
    parser = argparse.ArgumentParser(prog='speedster')
    parser.add_argument("config", type=str, help="Path to config.")
    parser.add_argument("-f", "--flows-per-batch", type=int, help="Max number of flows per batch.")
    parser.add_argument("-b", "--n-batches", type=int, help="Number of batches.")
    parser.add_argument("-w", "--n-workers", type=int, help="Number of workers")
    args = parser.parse_args()
    config_path = args.config

    pipeline_config = load_pipeline_configuration(config_path)
    enabled_flows = load_enabled_flows(pipeline_config)
    session = get_database_session(engine_kwargs=dict(isolation_level="READ COMMITTED"))

    if args.n_workers is None:
        args.n_workers = os.cpu_count()

    if args.flows_per_batch is None:
        n_cores = args.n_workers
    else:
        n_cores = min(args.n_workers, args.flows_per_batch)

    n_batches_run = 0
    with multiprocessing.Pool(n_cores, initializer=worker_init, initargs=(config_path,)) as p:
        print("Beginning fetch-run loop; press Ctrl-C to exit and allow time for cleanup")
        if args.flows_per_batch:
            print(f"Will cap at {args.flows_per_batch} flows per batch")
        if args.n_batches:
            print(f"Will stop after {args.n_batches} batches")
        while True:
            batch_of_flows, batch_types, count_per_type = gather_planned_flows(
                    session, enabled_flows, args.flows_per_batch)

            if len(batch_of_flows) == 0:
                print("No pending flows found---will wait a bit and try again")
                time.sleep(60*2)
            else:
                print("Batch contents: ", end='')
                count_report = []
                for type in sorted(count_per_type.keys()):
                    print(f"{count_per_type[type]} of {type}, ", end='')
                print()
                with tqdm(total=len(batch_of_flows)) as pbar:
                    try:
                        for _ in p.imap_unordered(worker_run_flow, zip(batch_of_flows, batch_types)):
                            pbar.update()
                    except KeyboardInterrupt:
                        print("Halting")
                        break
            n_batches_run += 1
            if args.n_batches and n_batches_run >= args.n_batches:
                break