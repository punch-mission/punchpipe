import os
import time
import argparse
import traceback
import subprocess
from pathlib import Path
from datetime import datetime

from prefect import serve
from prefect.variables import Variable

from punchpipe.control.health import update_machine_health_stats
from punchpipe.control.launcher import launcher_flow
from punchpipe.control.util import load_pipeline_configuration
from punchpipe.flows.fcorona import f_corona_process, f_corona_scheduler
from punchpipe.flows.level0 import level0_form_images, level0_ingest_raw_packets
from punchpipe.flows.level1 import level1_process_flow, level1_scheduler_flow
from punchpipe.flows.level2 import level2_process_flow, level2_scheduler_flow
from punchpipe.flows.level3 import (
    level3_PIM_process_flow,
    level3_PIM_scheduler_flow,
    level3_PTM_process_flow,
    level3_PTM_scheduler_flow,
)
from punchpipe.flows.levelq import levelq_process_flow, levelq_scheduler_flow
from punchpipe.flows.starfield import starfield_process_flow, starfield_scheduler_flow
from punchpipe.monitor.app import create_app

THIS_DIR = os.path.dirname(__file__)
app = create_app()
server = app.server

def main():
    """Run the PUNCH automated pipeline"""
    # mp.set_start_method('spawn')
    parser = argparse.ArgumentParser(prog='punchpipe')
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser('run', help="Run the pipeline.")
    run_parser.add_argument("config", type=str, help="Path to config.")
    args = parser.parse_args()

    if args.command == 'run':
        run(args.config)
    else:
        parser.print_help()

def serve_flows(configuration_path):
    config = load_pipeline_configuration.fn(configuration_path)
    launcher_deployment = launcher_flow.to_deployment(name="launcher-deployment",
                                                      description="Launch a pipeline segment.",
                                                      cron=config['launcher'].get("schedule", "* * * * *"),
                                                      parameters={"pipeline_configuration_path": configuration_path}
                                                      )

    level0_ingest_raw_packets_deployment = level0_ingest_raw_packets.to_deployment(name="level0_ingest_raw_packets",
                                                                                   description="Ingest raw packets.",
                                                                                   cron="*/5 * * * *",
                                                                                   parameters={
                                                                                       "pipeline_config_path": configuration_path},
                                                                                   tags=['L0'])

    level0_form_images_deployment = level0_form_images.to_deployment(name="level0_form_images",
                                                                     description="Form images from packets.",
                                                                     cron="*/5 * * * *",
                                                                     tags=['L0'],
                                                                     parameters={"pipeline_config_path": configuration_path})

    level1_scheduler_deployment = level1_scheduler_flow.to_deployment(name="level1-scheduler-deployment",
                                                                      description="Schedule a Level 1 flow.",
                                                                      tags=["L1", "scheduler"],
                                                                      cron=config['levels']['level1_process_flow'].get("schedule", "* * * * *"),
                                                                      parameters={"pipeline_config_path": configuration_path}
                                                                      )
    level1_process_deployment = level1_process_flow.to_deployment(name="level1_process_flow",
                                                                  tags=["L1", "process"],
                                                                  description="Process a file from Level 0 to Level 1.",
                                                                  parameters={"pipeline_config_path": configuration_path}
                                                                  )

    level2_scheduler_deployment = level2_scheduler_flow.to_deployment(name="level2-scheduler-deployment",
                                                                      description="Schedule a Level 2 flow.",
                                                                      tags=["L2", "scheduler"],
                                                                      cron=config['levels']['level2_process_flow'].get("schedule", "* * * * *"),
                                                                      parameters={
                                                                          "pipeline_config_path": configuration_path}

                                                                      )
    level2_process_deployment = level2_process_flow.to_deployment(name="level2_process_flow",
                                                                  description="Process files from Level 1 to Level 2.",
                                                                  tags=["L2", "process"],
                                                                  parameters={"pipeline_config_path": configuration_path}
                                                                  )

    levelq_scheduler_deployment = levelq_scheduler_flow.to_deployment(name="levelq-scheduler-deployment",
                                                                      description="Schedule a Level Q flow.",
                                                                      tags=["LQ", "scheduler"],
                                                                      cron=config['levels']['levelq_process_flow'].get("schedule", "* * * * *"),
                                                                      parameters={
                                                                          "pipeline_config_path": configuration_path}

                                                                      )
    levelq_process_deployment = levelq_process_flow.to_deployment(name="levelq_process_flow",
                                                                  description="Process files from Level 1 to Level Q.",
                                                                  tags=["LQ", "process"],
                                                                  parameters={"pipeline_config_path": configuration_path}
                                                                  )

    level3_PIM_scheduler_deployment = level3_PIM_scheduler_flow.to_deployment(name="level3-PIM-scheduler-deployment",
                                                                              description="Schedule a Level 3 flow to make PIM.",
                                                                              tags=["L3", "scheduler"],
                                                                              cron=config['levels']['level3_PIM_process_flow'].get("schedule", "* * * * *"),
                                                                              parameters={
                                                                                  "pipeline_config_path": configuration_path}

                                                                              )
    level3_PIM_process_deployment = level3_PIM_process_flow.to_deployment(name="level3_PIM_process_flow",
                                                                          description="Process to PIM files.",
                                                                          tags=["L3", "process"],
                                                                          parameters={
                                                                              "pipeline_config_path": configuration_path}
                                                                          )

    f_corona_scheduler_dep = f_corona_scheduler.to_deployment(name="construct_f_corona_background-scheduler-deployment",
                                                              description="Schedule an F corona background.",
                                                              cron=config['levels']['construct_f_corona_background_process_flow'].get("schedule", "* * * * *"),
                                                              tags=["L3", "scheduler"],
                                                              parameters={
                                                                                  "pipeline_config_path": configuration_path}

                                                              )
    f_corona_process_dep = f_corona_process.to_deployment(name="construct_f_corona_background_process_flow",
                                                          description="Process F corona background.",
                                                          tags=["L3", "process"],
                                                          parameters={
                                                                              "pipeline_config_path": configuration_path}
                                                          )

    starfield_scheduler_dep = starfield_scheduler_flow.to_deployment(name="construct_starfield-scheduler-deployment",
                                                                     description="Schedule a starfield background.",
                                                                     cron=config['levels']['construct_starfield_background_process_flow'].get("schedule", "* * * * *"),
                                                                     tags=["L3", "scheduler"],
                                                                     parameters={"pipeline_config_path": configuration_path}
                                                                                                 )

    starfield_process_dep = starfield_process_flow.to_deployment(name="construct_starfield_background_process_flow",
                                                                 description="Create starfield background.",
                                                                 tags=["L3", "process"],
                                                                 parameters={"pipeline_config_path": configuration_path}
                                                                 )


    level3_PTM_scheduler_deployment = level3_PTM_scheduler_flow.to_deployment(name="level3-PTM-scheduler-deployment",
                                                                              description="Schedule a Level 3 flow to make PTM.",
                                                                              cron=config['levels']['level3_PTM_process_flow'].get("schedule", "* * * * *"),
                                                                              tags=["L3", "scheduler"],
                                                                              parameters={
                                                                                  "pipeline_config_path": configuration_path}

                                                                              )
    level3_PTM_process_deployment = level3_PTM_process_flow.to_deployment(name="level3_PTM_process_flow",
                                                                          description="Process PTM files from Level 2 to Level 3.",
                                                                          tags=["L3", "process"],
                                                                          parameters={
                                                                              "pipeline_config_path": configuration_path}
                                                                          )

    health_deployment = update_machine_health_stats.to_deployment(name="update-health-stats-deployment",
                                                                  description="Update the health stats table data.",
                                                                  cron="* * * * *")

    serve(launcher_deployment,
          level0_form_images_deployment, level0_ingest_raw_packets_deployment,
          level1_scheduler_deployment, level1_process_deployment,
          level2_scheduler_deployment, level2_process_deployment,
          levelq_scheduler_deployment, levelq_process_deployment,
          level3_PTM_scheduler_deployment, level3_PTM_process_deployment,
          f_corona_process_dep, f_corona_scheduler_dep,
          starfield_process_dep, starfield_scheduler_dep,
          level3_PIM_scheduler_deployment, level3_PIM_process_deployment,
          health_deployment,
          limit=1000
          )

def run(configuration_path):
    now = datetime.now()

    configuration_path = str(Path(configuration_path).resolve())
    output_path = f"punchpipe_{now.strftime('%Y%m%d_%H%M%S')}.txt"

    print()
    print(f"Launching punchpipe at {now} with configuration: {configuration_path}")
    print(f"Terminal logs from punchpipe are in {output_path}")


    with open(output_path, "w") as f:
        try:
            prefect_process = subprocess.Popen(["prefect", "server", "start"],
                                               stdout=f, stderr=subprocess.STDOUT)
            time.sleep(10)
            monitor_process = subprocess.Popen(["gunicorn",
                                                "-b", "0.0.0.0:8050",
                                                "--chdir", THIS_DIR,
                                                "cli:server"],
                                               stdout=f, stderr=subprocess.STDOUT)
            Variable.set("punchpipe_config", configuration_path, overwrite=True)
            print("Launched Prefect dashboard on http://localhost:4200/")
            print("Launched punchpipe monitor on http://localhost:8050/")
            print("Use ctrl-c to exit.")

            serve_flows(configuration_path)
            prefect_process.wait()
            monitor_process.wait()
        except KeyboardInterrupt:
            print("Shutting down.")
            prefect_process.terminate()
            time.sleep(5)
            monitor_process.terminate()
            print()
            print("punchpipe safely shut down.")
        except Exception as e:
            print(f"Received error: {e}")
            print(traceback.format_exc())
            prefect_process.terminate()
            time.sleep(5)
            monitor_process.terminate()
            print()
            print("punchpipe abruptly shut down.")

if __name__ == "__main__":
    monitor_process = subprocess.Popen(["gunicorn",
                                        "-b", "0.0.0.0:8050",
                                        "--chdir", THIS_DIR,
                                        "cli:server"])
