# punchpipe
`punchpipe` is the data processing pipeline for [the PUNCH mission](https://punch.space.swri.edu/). 
It relies heavily on the more generic `controlsegment` package to implement its pipeline, which in 
turn is built on [`prefect`](https://www.prefect.io/). 

The `punchpipe` is organized into segments, i.e. levels of processing to produce specific
data products. Segments are referred in code by their ending level, 
e.g. `level1` means the Level 0 to Level 1 segment. 

## Accessing the data

## Running

## Contributing

## Licensing