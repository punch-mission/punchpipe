root_path="/Users/jhughes/Desktop/repos/punchpipe"

rm *-deployment.yaml

prefect deployment build $root_path/punchpipe/flows/level1.py:level1_process_flow -n level1_process_flow
prefect deployment build $root_path/punchpipe/flows/level1.py:level1_scheduler_flow -n level1_scheduler_flow
prefect deployment build $root_path/punchpipe/controlsegment/launcher.py:launcher_flow -n launcher_flow
prefect deployment apply level1_process_flow-deployment.yaml
prefect deployment apply level1_scheduler_flow-deployment.yaml
prefect deployment apply launcher_flow-deployment.yaml