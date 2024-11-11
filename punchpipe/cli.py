import subprocess

import click
from waitress import serve

from .monitor.app import server


@click.command
def run():
    print("Launching punchpipe monitor on http://localhost:8050/.")
    subprocess.Popen(["prefect", "server", "start"])
    serve(server, host='0.0.0.0', port=8050)
    print("\npunchpipe Prefect flows must be stopped manually in Prefect.")
