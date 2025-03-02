# punchpipe

`punchpipe` is the data processing pipeline for [the PUNCH mission](https://punch.space.swri.edu/).
All the science code and actual calibration functionality lives in [ppunchbowl](https://github.com/punch-mission/punchbowl).
This package automates the control segment for the Science Operations Center.

> [!CAUTION]
> This package will likely have breaking changes during commissioning (the first few months after launch).
> Stability is not promised until v1.

The `punchpipe` is organized into segments, i.e. levels of processing to produce specific
data products. Segments are referred in code by their ending level,
e.g. `level1` means the Level 0 to Level 1 segment.

## First-time setup

Coming soon.

## Running

`punchpipe run config.yaml`

## Testing

1. Install Podman Desktop using your preferred method
2. Pull the mariadb image with `podman pull docker.io/library/mariadb`
3. Run tests with `pytest`

## Getting help

Please open an issue or discussion on this repo.

## Contributing

We encourage all contributions.
If you have a problem with the code or would like to see a new feature, please open an issue.
Or you can submit a pull request.
