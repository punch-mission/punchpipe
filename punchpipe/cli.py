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

@main.command
def run():
    print("Launching punchpipe monitor on http://localhost:8051/.")
    subprocess.Popen(["prefect", "server", "start"])
    print("\npunchpipe Prefect flows must be stopped manually in Prefect.")
    mp.Process(target=launch_monitor, args=()).start()
