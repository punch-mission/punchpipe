# punchpipe

> [!IMPORTANT]  
> punchpipe has been merged into punchbowl in the `auto` subpackage. This separate repo is no longer maintained and is archived. 

`punchpipe` is the data processing pipeline for [the PUNCH mission](https://punch.space.swri.edu/).
All the science code and actual calibration functionality lives in [punchbowl](https://github.com/punch-mission/punchbowl).
This package automates the control segment for the Science Operations Center.

The `punchpipe` is organized into segments, i.e. levels of processing to produce specific
data products. Segments are referred in code by their ending level,
e.g. `level1` means the Level 0 to Level 1 segment.
