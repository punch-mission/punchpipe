from prefect.deployments import Deployment
from prefect.deployments import FlowScript
from prefect.orion.schemas.schedules import IntervalSchedule
from datetime import timedelta
from pathlib import Path

# Deployment(
#     flow=FlowScript(path="/path/to/hello_flow.py", name="hello_world"),
#     name="Hello World-unscheduled",
#     parameters={"name": "Trillian"},
#     tags=["trillian","ad-hoc"],
# )

# Deployment(
#     flow=FlowScript(path="/path/to/hello_flow.py", name="hello_world"),
#     name="Hello World-daily",
#     schedule=IntervalSchedule(interval=timedelta(days=1)),
#     parameters={"name": "Arthur"},
#     tags=["arthur","daily"],
# )

# Deployment(
#     flow=FlowScript(path="/path/to/hello_flow.py", name="hello_world"),
#     name="Hello World-weekly",
#     schedule=IntervalSchedule(interval=timedelta(weeks=1)),
#     parameters={"name": "Marvin"},
#     tags=["marvin","weekly"],
# )

Deployment(
    flow=FlowScript(path= Path.cwd() / "punchpipe" / "level1" / "flow.py", name="level1-core-flow"), 
    name="Level 1 Core Flow",
    parameters={"input_filename":"test.fits", "output_directory":"."}
)
