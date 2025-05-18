import os
import time
import inspect
import argparse
import traceback
import subprocess
from pathlib import Path
from datetime import datetime
from importlib import import_module

from prefect import Flow, serve
from prefect.client.schemas.objects import ConcurrencyLimitConfig, ConcurrencyLimitStrategy
from prefect.variables import Variable

from punchpipe.control.util import load_pipeline_configuration
from punchpipe.monitor.app import create_app

THIS_DIR = os.path.dirname(__file__)
app = create_app()
server = app.server

def main():
    """Run the PUNCH automated pipeline"""
    parser = argparse.ArgumentParser(prog='punchpipe')
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser('run', help="Run the pipeline.")
    serve_control_parser = subparsers.add_parser('serve-control', help="Serve the control flows.")
    serve_data_parser = subparsers.add_parser('serve-data', help="Serve the data-processing flows.")

    run_parser.add_argument("config", type=str, help="Path to config.")
    run_parser.add_argument("--launch-prefect", action="store_true", help="Launch the prefect server")
    serve_control_parser.add_argument("config", type=str, help="Path to config.")
    serve_data_parser.add_argument("config", type=str, help="Path to config.")
    args = parser.parse_args()

    if args.command == 'run':
        run(args.config, args.launch_prefect)
    elif args.command == 'serve-data':
        run_data(args.config)
    elif args.command == 'serve-control':
        run_control(args.config)
    else:
        parser.print_help()

def find_flow(target_flow, subpackage="flows") -> Flow:
    for filename in os.listdir(os.path.join(THIS_DIR, subpackage)):
        if filename.endswith(".py"):
            module_name = f"punchpipe.{subpackage}."  + os.path.splitext(filename)[0]
            module = import_module(module_name)
            for name, obj in inspect.getmembers(module):
                if name == target_flow:
                    return obj
    else:
        raise RuntimeError(f"No flow found for {target_flow}")

def construct_flows_to_serve(configuration_path, include_data=True, include_control=True):
    config = load_pipeline_configuration(configuration_path)

    # create each kind of flow. add both the scheduler and process flow variant of it.
    flows_to_serve = []
    if include_data:
        for flow_name in config["flows"]:
            # first we deploy the scheduler flow
            specific_name = flow_name + "_scheduler_flow"
            specific_tags = config["flows"][flow_name].get("tags", [])
            specific_description = config["flows"][flow_name].get("description", "")
            flow_function = find_flow(specific_name)
            flow_deployment = flow_function.to_deployment(
                name=specific_name,
                description="Scheduler: " + specific_description,
                tags = ["scheduler"] + specific_tags,
                cron=config['flows'][flow_name].get("schedule", None),
                concurrency_limit=ConcurrencyLimitConfig(
                    limit=1,
                    collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW
                ),
                parameters={"pipeline_config_path": configuration_path}
            )
            flows_to_serve.append(flow_deployment)

            # then we deploy the corresponding process flow
            specific_name = flow_name + "_process_flow"
            flow_function = find_flow(specific_name)
            concurrency_value = config["flows"][flow_name].get("concurrency_limit", None)
            concurrency_config = ConcurrencyLimitConfig(
                    limit=concurrency_value,
                    collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW
                ) if concurrency_value else None
            flow_deployment = flow_function.to_deployment(
                name=specific_name,
                description="Process: " + specific_description,
                tags = ["process"] + specific_tags,
                parameters={"pipeline_config_path": configuration_path},
                concurrency_limit=concurrency_config
            )
            flows_to_serve.append(flow_deployment)

    if include_control:
        # there are special control flows that manage the pipeline instead of processing data
        # time to kick those off!
        for flow_name in config["control"]:
            flow_function = find_flow(flow_name, "control")
            concurrency_config = ConcurrencyLimitConfig(
                    limit=1,
                    collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW
                )
            flow_deployment = flow_function.to_deployment(
                name=flow_name,
                description=config["control"][flow_name].get("description", ""),
                tags=["control"],
                cron=config['control'][flow_name].get("schedule", "* * * * *"),
                parameters={"pipeline_config_path": configuration_path},
                concurrency_limit=concurrency_config
            )
            flows_to_serve.append(flow_deployment)
    return flows_to_serve

def run_data(configuration_path):
    configuration_path = str(Path(configuration_path).resolve())
    serve(*construct_flows_to_serve(configuration_path, include_control=False, include_data=True))

def run_control(configuration_path):
    configuration_path = str(Path(configuration_path).resolve())
    serve(*construct_flows_to_serve(configuration_path, include_control=True, include_data=False))

def run(configuration_path, launch_prefect=False):
    now = datetime.now()

    configuration_path = str(Path(configuration_path).resolve())
    output_path = f"punchpipe_{now.strftime('%Y%m%d_%H%M%S')}.txt"

    print()
    print(f"Launching punchpipe at {now} with configuration: {configuration_path}")
    print(f"Terminal logs from punchpipe are in {output_path}")


    with open(output_path, "a") as f:
        shutdown_expected = False
        prefect_process = None
        prefect_services_process = None
        cluster_process = None
        data_process = None
        control_process = None
        try:
            numa_prefix_0 = ['numactl', '--membind', '0', '--cpunodebind', '0']
            numa_prefix_1 = ['numactl', '--membind', '1', '--cpunodebind', '1']
            if launch_prefect:
                print("Launcing prefect")
                prefect_process = subprocess.Popen(
                    [*numa_prefix_0, "prefect", "server", "start", "--no-services"], stdout=f, stderr=f)
                time.sleep(5)
                # Separating the server and the background services may help avoid overwhelming the database connections
                # https://github.com/PrefectHQ/prefect/issues/16299#issuecomment-2698732783
                prefect_services_process = subprocess.Popen(
                    [*numa_prefix_0, "prefect", "server", "services", "start"], stdout=f, stderr=f)

            cluster_process = subprocess.Popen([*numa_prefix_1, 'punchpipe_cluster', configuration_path],
                                               stdout=f, stderr=f)
            monitor_process = subprocess.Popen([*numa_prefix_0, "gunicorn",
                                                "-b", "0.0.0.0:8050",
                                                "--chdir", THIS_DIR,
                                                "cli:server"],
                                               stdout=f, stderr=f)
            time.sleep(1)
            Variable.set("punchpipe_config", configuration_path, overwrite=True)

            # These processes send a _lot_ of output, so we let it go to the screen instead of making the log file
            # enormous
            def data_process_launcher() -> subprocess.Popen:
                return subprocess.Popen([*numa_prefix_1, "punchpipe", "serve-data", configuration_path])

            def control_process_launcher() -> subprocess.Popen:
                return subprocess.Popen([*numa_prefix_0, "punchpipe", "serve-control", configuration_path])

            data_process = data_process_launcher()
            control_process = control_process_launcher()

            if launch_prefect is not None:
                print("Launched Prefect dashboard on http://localhost:4200/")
            print("Launched punchpipe monitor on http://localhost:8050/")
            print("Launched dask cluster on http://localhost:8786/")
            print("Dask dashboard available at http://localhost:8787/")
            print("Use ctrl-c to exit.")

            time.sleep(10)
            while True:
                # `.poll()` updates but does not return the object's returncode attribute
                cluster_process.poll()
                control_process.poll()
                data_process.poll()
                if launch_prefect:
                    prefect_process.poll()
                    prefect_services_process.poll()
                    if prefect_process.returncode or prefect_services_process.returncode:
                        print("Prefect process exited unexpectedly")
                        break
                if cluster_process.returncode:
                    print("Cluster process exited unexpectedly")
                    break
                # Core processes are still running. Now check worker processes, which we can restart safely
                if control_process.returncode:
                    print(f"Restarted control process at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    control_process = control_process_launcher()
                if data_process.returncode:
                    print(f"Restarted data process at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    data_process = data_process_launcher()
                time.sleep(10)
            raise RuntimeError()
        except KeyboardInterrupt:
            print("Shutting down.")
            shutdown_expected = True
        except Exception as e:
            print(f"Received error: {e}")
            print(traceback.format_exc())
        finally:
            control_process.terminate() if control_process else None
            data_process.terminate() if data_process else None
            control_process.wait() if control_process else None
            data_process.wait() if data_process else None
            time.sleep(1)
            if launch_prefect:
                prefect_services_process.terminate() if prefect_services_process else None
                prefect_services_process.wait() if prefect_services_process else None
                time.sleep(3)
                prefect_process.terminate() if prefect_process else None
                prefect_process.wait() if prefect_process else None
                time.sleep(3)
            cluster_process.terminate() if cluster_process else None
            monitor_process.terminate() if monitor_process else None
            cluster_process.wait() if cluster_process else None
            monitor_process.wait() if monitor_process else None
            print()
            if shutdown_expected:
                print("punchpipe safely shut down")
            else:
                print("punchpipe abruptly shut down")
