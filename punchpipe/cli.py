import click
from waitress import serve
import subprocess

from .monitor.app import create_app

@click.command
def run():
    print("Launching punchpipe monitor on http://localhost:8050/.")
    subprocess.Popen(["prefect", "server", "start"])
    # serve(server, host='0.0.0.0', port=8050)
    print("\npunchpipe Prefect flows must be stopped manually in Prefect.")

    app = create_app()
    app.run_server(debug=False, port=8051)

    # if not math.isinf(menu.duration):
    #     time.sleep(menu.duration)
    #     sys.exit(0)