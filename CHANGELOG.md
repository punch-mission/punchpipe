# Changelog

[This is available in GitHub](https://github.com/punch-mission/punchpipe/releases)

## Version 0.0.10: July 3, 2025

* Fixes metadata usage for quicklook animations in https://github.com/punch-mission/punchpipe/pull/185
* Relabels CCD halves in https://github.com/punch-mission/punchpipe/pull/174
* Don't remake existing files in https://github.com/punch-mission/punchpipe/pull/188
* Save an L1-with-stray-light intermediate file in https://github.com/punch-mission/punchpipe/pull/193
* Support separated LQ flows and exclude NFI from LQ CTM in https://github.com/punch-mission/punchpipe/pull/192
* Clear .jp2, .sha, and parent directories in cleaner flow in https://github.com/punch-mission/punchpipe/pull/191
* L2s and LQ CTMs with missing input files can be made anyway after a certain number of days in https://github.com/punch-mission/punchpipe/pull/194
* Reduces L2 code duplication in https://github.com/punch-mission/punchpipe/pull/195
* Set date_obs correctly in DB for L2s, and update cleaner flow for L2 in https://github.com/punch-mission/punchpipe/pull/196
* Temporarily disabled NFI in L2s in https://github.com/punch-mission/punchpipe/pull/197

## Version 0.0.9: June 4, 2025

* Relabels the polarizers for WFI for the flipped orientation in https://github.com/punch-mission/punchpipe/pull/179

## Version 0.0.8: June 3, 2025

* Group L2 and LQ inputs with time flexiblity, check there are enough files to fit for LQ, and fix L2 input file queries in https://github.com/punch-mission/punchpipe/pull/170
* Many changes for QuickPUNCH compatibility in https://github.com/punch-mission/punchpipe/pull/175

## Version 0.0.7: May 22, 2025

* Disables logging in cache manager if not in a flow context in https://github.com/punch-mission/punchpipe/pull/159
* Weighted launching and NUMA configuration in https://github.com/punch-mission/punchpipe/pull/160
* Save floating-point values for COMPBITS when appropriate in L0 in https://github.com/punch-mission/punchpipe/pull/164
* Updates precision of square root decoding table bitrate in https://github.com/punch-mission/punchpipe/pull/167
* LQ speedup, PCA support, and fixes to DB file state in https://github.com/punch-mission/punchpipe/pull/168
* Adds script to add calibration files to database and adds distortion to Level 1 processing in https://github.com/punch-mission/punchpipe/pull/169

## Version 0.0.6: May 12, 2025

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
* Added a "cleaner" flow, staggered flow launching, and L2 fixes in https://github.com/punch-mission/punchpipe/pull/145
* Add flow throughput and duration stats to the dashboard in https://github.com/punch-mission/punchpipe/pull/144
* Expands ffmpeg movie creation options in https://github.com/punch-mission/punchpipe/pull/147
* Appropriately uses TAI time in https://github.com/punch-mission/punchpipe/pull/146
* Stores quicklook movies in date-based file structure in https://github.com/punch-mission/punchpipe/pull/150
* Checks that input files for quicklook movies are sorted and only schedules if files are found in https://github.com/punch-mission/punchpipe/pull/151
* Updates L0 header generation in https://github.com/punch-mission/punchpipe/pull/156
* Uses the shared memory cache for simpunch and sets date_created for files in the database in https://github.com/punch-mission/punchpipe/pull/154
* Improvements to dashboard status cards and flow table in https://github.com/punch-mission/punchpipe/pull/155

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
