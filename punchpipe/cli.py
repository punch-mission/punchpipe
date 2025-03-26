import os
import time
import inspect
import argparse
import traceback
import subprocess
from pathlib import Path
from datetime import datetime, UTC
from importlib import import_module

from prefect import Flow, serve
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
    run_parser.add_argument("config", type=str, help="Path to config.")
    args = parser.parse_args()

    if args.command == 'run':
        run(args.config)
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

def serve_flows(configuration_path):
    config = load_pipeline_configuration.fn(configuration_path)

    # create each kind of flow. add both the scheduler and process flow variant of it.
    flows_to_serve = []
    for flow_name in config["flows"]:
        specific_name = flow_name + "_scheduler_flow"
        specific_tags = config["flows"][flow_name].get("tags", [])
        specific_description = config["flows"][flow_name].get("description", "")
        flow_function = find_flow(specific_name)
        flow_deployment = flow_function.to_deployment(
            name=specific_name,
            description="Scheduler: " + specific_description,
            tags = ["scheduler"] + specific_tags,
            cron=config['flows'][flow_name].get("schedule", "* * * * *"),
            parameters={"pipeline_config_path": configuration_path}
        )
        flows_to_serve.append(flow_deployment)

        specific_name = flow_name + "_process_flow"
        flow_function = find_flow(specific_name)
        flow_deployment = flow_function.to_deployment(
            name=specific_name,
            description="Process: " + specific_description,
            tags = ["process"] + specific_tags,
            parameters={"pipeline_config_path": configuration_path}
        )
        flows_to_serve.append(flow_deployment)

    # there are special control flows that manage the pipeline instead of processing data
    # time to kick those off!
    for flow_name in config["control"]:
        flow_function = find_flow(flow_name, "control")
        flow_deployment = flow_function.to_deployment(
            name=flow_name,
            description=config["control"][flow_name].get("description", ""),
            tags=["control"],
            cron=config['control'][flow_name].get("schedule", "* * * * *"),
            parameters={"pipeline_config_path": configuration_path}
        )
        flows_to_serve.append(flow_deployment)
    return flows_to_serve
    # serve(*flows_to_serve, limit=1000)

def run(configuration_path):
    now = datetime.now(UTC)

    configuration_path = str(Path(configuration_path).resolve())
    output_path = f"punchpipe_{now.strftime('%Y%m%d_%H%M%S')}.txt"

    print()
    print(f"Launching punchpipe at {now} with configuration: {configuration_path}")
    print(f"Terminal logs from punchpipe are in {output_path}")


    with open(output_path, "a") as f:
        try:
            prefect_process = subprocess.Popen(["prefect", "server", "start"],
                                               stdout=f, stderr=f)
            time.sleep(10)
            monitor_process = subprocess.Popen(["gunicorn",
                                                "-b", "0.0.0.0:8050",
                                                "--chdir", THIS_DIR,
                                                "cli:server"],
                                               stdout=f, stderr=f)
            Variable.set("punchpipe_config", configuration_path, overwrite=True)
            print("Launched Prefect dashboard on http://localhost:4200/")
            print("Launched punchpipe monitor on http://localhost:8050/")
            print("Use ctrl-c to exit.")

            serve(*serve_flows(configuration_path), limit=1000)

            prefect_process.wait()
            monitor_process.wait()
        except KeyboardInterrupt:
            print("Shutting down.")
            prefect_process.terminate()
            prefect_process.wait()
            time.sleep(5)
            monitor_process.terminate()
            monitor_process.wait()
            print()
            print("punchpipe safely shut down.")
        except Exception as e:
            print(f"Received error: {e}")
            print(traceback.format_exc())
            prefect_process.terminate()
            prefect_process.wait()
            time.sleep(5)
            monitor_process.terminate()
            monitor_process.wait()
            print()
            print("punchpipe abruptly shut down.")
