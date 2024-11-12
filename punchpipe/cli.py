import subprocess
import multiprocessing as mp

import click
from prefect import serve

from punchpipe.controlsegment.launcher import launcher_flow
from punchpipe.flows.level1 import level1_process_flow, level1_scheduler_flow
from punchpipe.flows.level2 import level2_process_flow, level2_scheduler_flow
from punchpipe.flows.level3 import level3_PTM_process_flow, level3_PTM_scheduler_flow
from punchpipe.flows.levelq import levelq_process_flow, levelq_scheduler_flow
from .monitor.app import create_app


@click.group
def main():
    """Run the PUNCH automated pipeline"""

def launch_monitor():
    app = create_app()
    app.run_server(debug=False, port=8051)

def deploy():
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
def run():
    print("Launching punchpipe monitor on http://localhost:8051/.")
    subprocess.Popen(["prefect", "server", "start"])
    print("\npunchpipe Prefect flows must be stopped manually in Prefect.")
    mp.Process(target=launch_monitor, args=()).start()
    deploy()