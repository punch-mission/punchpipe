# Changelog

[This is available in GitHub](https://github.com/punch-mission/punchpipe/releases)

## Latest: unreleased

* Prepares punchpipe for SOC2NOAA Interface by @jmbhughes in https://github.com/punch-mission/punchpipe/pull/94
* Specify path for codecov by @jmbhughes in https://github.com/punch-mission/punchpipe/pull/95
* Update issue templates by @jmbhughes in https://github.com/punch-mission/punchpipe/pull/97
* Update README.md by @jmbhughes in https://github.com/punch-mission/punchpipe/pull/98
* Adds vignetting to level 1 processing by @lowderchris in https://github.com/punch-mission/punchpipe/pull/103
* Makes AWS profile configurable by @jmbhughes in https://github.com/punch-mission/punchpipe/pull/112
* Added notes in README about testing in https://github.com/punch-mission/punchpipe/pull/114
* Creates VAM/VAN flow automation, corrects flash block length, fixes attitude quaternions in https://github.com/punch-mission/punchpipe/pull/102
* Checked that all times were UTC in https://github.com/punch-mission/punchpipe/pull/119
* Many automation improvements in https://github.com/punch-mission/punchpipe/pull/115
* Uses central dask cluster in https://github.com/punch-mission/punchpipe/pull/129
* Improves level 0 metadata population in https://github.com/punch-mission/punchpipe/pull/128
* Splits CCD parameters per chip half in https://github.com/punch-mission/punchpipe/pull/131
* Iterates over sequence count instead of packet index in https://github.com/punch-mission/punchpipe/pull/132
* Varied improvements to the pipeline, including launching and scheduling in https://github.com/punch-mission/punchpipe/pull/134
* Fixed database entries for simpunch and launching improvements in https://github.com/punch-mission/punchpipe/pull/135
* Added a shared memory cache, streamlined the launcher, improved robustness, and changed logging to local time in https://github.com/punch-mission/punchpipe/pull/136
* Add flow throughput and duration stats to the dashboard in https://github.com/punch-mission/punchpipe/pull/144
* Expands ffmpeg movie creation options in https://github.com/punch-mission/punchpipe/pull/147

## Version 0.0.5: Jan 3, 2025

- if sequence counters don't increase properly, call it a bad image by @jmbhughes in #90

## Version 0.0.4: Dec 19, 2024

- Updates for V4 RFR2

## Version 0.0.3: Dec 11, 2024

- Fix l0 image form by @jmbhughes in #87
- Improve l0 by @jmbhughes in #88

## Version 0.0.2: Dec 2, 2024

- Improve monitoring utility by @jmbhughes in #78
- Prepare for End2End Test by @jmbhughes in #81
- Fix reference times for f corona models by @jmbhughes in #83
- fully tested level 0 by @jmbhughes in #84
- save only the TLM filename instead of the whole path by @jmbhughes in #85
- make sure the path is extracted for tlm by @jmbhughes in #86

## Version 0.0.1: Nov 2, 2024

Initial Release
