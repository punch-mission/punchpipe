# punchpipe
`punchpipe` is the data processing pipeline for [the PUNCH mission](https://punch.space.swri.edu/). 
All the science code and actual calibration functionality lives in `punchbowl`. This package
only automates the control segment for the Science Operations Center. 

The `punchpipe` is organized into segments, i.e. levels of processing to produce specific
data products. Segments are referred in code by their ending level, 
e.g. `level1` means the Level 0 to Level 1 segment. 

## Accessing the data

## First-time setup
1. Create a clean virtual environment. You can do this with conda using `conda env create --name ENVIRONMENT-NAME` 
2. Install `punchbowl` using `pip install .` in the `punchbowl` directory. 
3. Install `punchpipe` using `pip install .` while in this directory 
4. Set up database credentials Prefect block by running `python scripts/credentials.py`. 
    - If this file does not exist for you. You need to determine your mySQL credentials then create a block in Python: 
    ```py
   from punchpipe.controlsegment.db import MySQLCredentials
   cred = MySQLCredentials(user="username", password="password")
   cred.save('mysql-cred')
    ```
5. Set up databases using the `setup_db.sql` in the `scripts` directory. 
6. Build all the necessary deployments for Prefect by follwing [these instructions](https://docs.prefect.io/concepts/deployments/).
   - See below for an example:
   ```shell
    prefect deployment build ./punchpipe/flows/level1.py:level1_process_flow -n level1-process-flow
    ```
7. Apply the deployments using the previous instructions
   ```shell
   prefect deployment apply ./level1_process_flow-deployment.yaml
   ```
8. Create a work queue in the Prefect UI for the deployments
9. Create an agent for the worflow by following instructions in the UI

## Running
1. Make sure first-time setup is complete
2. Launch Prefect using `prefect orion start`
3. Create agents for the work queues by following the instructions in the UI for the work queue

## Resetting
1. Reset the Prefect Orion database using `prefect orion database reset`. 
2. Remove all the `punchpipe` databases by running `erase_db.sql`

## Contributing
 
## Licensing