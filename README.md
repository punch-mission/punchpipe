# punchpipe

![example simulated image](example.png)

`punchpipe` is the data processing pipeline for [the PUNCH mission](https://punch.space.swri.edu/).
All the science code and actual calibration functionality lives in `punchbowl`. This package
only automates the control segment for the Science Operations Center.

> [!CAUTION]
> This package is still being developed. There will be breaking code changes until v1.
> We advise you to wait until then to use it.

The `punchpipe` is organized into segments, i.e. levels of processing to produce specific
data products. Segments are referred in code by their ending level,
e.g. `level1` means the Level 0 to Level 1 segment.

## Accessing the data

Coming soon.

## First-time setup

Coming soon.

## Running

Coming soon.

## Simulating observations

The simulation portion of punchpipe, called simpunch, accepts a total brightness and polarized brightness model as input.
These have been created using the
[FORWARD code](https://www.frontiersin.org/journals/astronomy-and-space-sciences/articles/10.3389/fspas.2016.00008/full)
from [GAMERA simulation data ](https://arxiv.org/pdf/2405.13069).
These images are fed backward through the pipeline from level 3 to level 0 products,
adding appropriate effects along the way.

## Getting help

Please open an issue or discussion on this repo.

## Contributing

We encourage all contributions.
If you have a problem with the code or would like to see a new feature, please open an issue.
Or you can submit a pull request.
