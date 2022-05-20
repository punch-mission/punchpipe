# punchpipe
`punchpipe` is the data processing pipeline for [the PUNCH mission](https://punch.space.swri.edu/). 
It relies heavily on the more generic `controlsegment` package to implement its pipeline, which in 
turn is built on [`prefect`](https://www.prefect.io/). 

The `punchpipe` is organized into segments, i.e. levels of processing to produce specific
data products. Segments are referred in code by their ending level, 
e.g. `level1` means the Level 0 to Level 1 segment. 

## Accessing the data

## Running

### Server Bound Execution
If you want to run this software on Prefect Server as we do in the Science Operations Center, 
you will need to set up the appropriate database structure.
1. Set up databases using the `setup_db.sql` in the `scripts` directory.
2. Make sure Postgres is not already running. 
3. Docker must be running to launch Prefect Server. So, begin by starting Docker.
4. Then launch Prefect Server using `prefect server start` in the command line. 
5. Create a Prefect Agent with `prefect agent local start`
6. Make sure the most recent version of `punchpipe` is installed by running `pip install .`
7. Run `scripts/initalize.py` to initialize/update the pipeline. 
## Contributing

## Licensing