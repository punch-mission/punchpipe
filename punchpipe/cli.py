import os
import time
import subprocess
from pathlib import Path
from datetime import datetime

import click
from prefect import flow, serve
from prefect.variables import Variable

from punchpipe.controlsegment.launcher import launcher_flow
from punchpipe.flows.level1 import level1_process_flow, level1_scheduler_flow
from punchpipe.flows.level2 import level2_process_flow, level2_scheduler_flow
from punchpipe.flows.level3 import level3_PTM_process_flow, level3_PTM_scheduler_flow
from punchpipe.flows.levelq import levelq_process_flow, levelq_scheduler_flow
from punchpipe.monitor.app import create_app

THIS_DIR = os.path.dirname(__file__)
app = create_app()
server = app.server

@click.group
def main():
    """Run the PUNCH automated pipeline"""


@flow
def my_flow():
    print("Hello, Prefect!")


def serve_flows():
    launcher_deployment = launcher_flow.to_deployment(name="launcher-deployment",
                                                      description="Launch a pipeline segment.",
                                                      cron="* * * * *",

                                                      )

    level1_scheduler_deployment = level1_scheduler_flow.to_deployment(name="level1-scheduler-deployment",
                                                                      description="Schedule a Level 1 flow.",
                                                                      cron="* * * * *",
                                                                      )
    level1_process_deployment = level1_process_flow.to_deployment(name="level1_process_flow",
                                                                  description="Process a file from Level 0 to Level 1.")

    level2_scheduler_deployment = level2_scheduler_flow.to_deployment(name="level2-scheduler-deployment",
                                                                      description="Schedule a Level 2 flow.",
                                                                      cron="* * * * *",
                                                                      )
    level2_process_deployment = level2_process_flow.to_deployment(name="level2_process_flow",
                                                                  description="Process files from Level 1 to Level 2.")

    levelq_scheduler_deployment = levelq_scheduler_flow.to_deployment(name="levelq-scheduler-deployment",
                                                                      description="Schedule a Level Q flow.",
                                                                      cron="* * * * *",
                                                                      )
    levelq_process_deployment = levelq_process_flow.to_deployment(name="levelq_process_flow",
                                                                  description="Process files from Level 1 to Level Q.")

    level3_PTM_scheduler_deployment = level3_PTM_scheduler_flow.to_deployment(name="level3-PTM-scheduler-deployment",
                                                                              description="Schedule a Level 3 flow to make PTM.",
                                                                              cron="* * * * *",
                                                                              )
    level3_PTM_process_deployment = level3_PTM_process_flow.to_deployment(name="level3_PTM_process_flow",
                                                                          description="Process PTM files from Level 2 to Level 3.")

    serve(launcher_deployment,
          level1_scheduler_deployment, level1_process_deployment,
          level2_scheduler_deployment, level2_process_deployment,
          levelq_scheduler_deployment, levelq_process_deployment,
          level3_PTM_scheduler_deployment, level3_PTM_process_deployment,
          limit=1000
    )


@main.command
@click.argument("configuration_path", type=click.Path(exists=True))
def run(configuration_path):
    now = datetime.now()

    configuration_path = str(Path(configuration_path).resolve())
    output_path = f"punchpipe_{now.strftime("%Y%m%d_%H%M%S")}.txt"

    print()
    print(f"Launching punchpipe at {now} with configuration: {configuration_path}")
    print(f"Terminal logs from punchpipe are in {output_path}.")
    print("punchpipe Prefect flows must be stopped manually in Prefect.")
    print("Launching Prefect dashboard on http://localhost:4200/.")
    print("Launching punchpipe monitor on http://localhost:8051/.")
    print("Use ctrl-c to exit.")

    with open(output_path, "w") as f:
        prefect_process = subprocess.Popen(["prefect", "server", "start"],
                                           stdout=f, stderr=subprocess.STDOUT)
        monitor_process = subprocess.Popen(["gunicorn",
                                            "-b", "0.0.0.0:8051",
                                            "--chdir", THIS_DIR,
                                            "cli:server"],
                                           stdout=f, stderr=subprocess.STDOUT)
        time.sleep(3)
        Variable.set("punchpipe_config", configuration_path, overwrite=True)
        serve_flows()

        try:
            prefect_process.wait()
            monitor_process.wait()
        except Exception:
            prefect_process.terminate()
            monitor_process.terminate()

    print()
    print("punchpipe shut down.")
