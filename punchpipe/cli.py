import subprocess
import multiprocessing as mp

import click

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
