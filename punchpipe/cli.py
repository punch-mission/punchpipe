import subprocess

import click

from .monitor.app import create_app


@click.group
def main():
    """Run the PUNCH automated pipeline"""

@main.command
def run():
    print("Launching punchpipe monitor on http://localhost:8050/.")
    subprocess.Popen(["prefect", "server", "start"])
    print("\npunchpipe Prefect flows must be stopped manually in Prefect.")

    app = create_app()
    app.run_server(debug=False, port=8051)
