from __future__ import annotations
from datetime import datetime
import json
import random
from prefect import Parameter
from prefect.tasks.mysql import MySQLFetch, MySQLExecute
from punchpipe.infrastructure.tasks.core import ScienceTask, IngestTask, OutputTask
from punchpipe.infrastructure.tasks.scheduler import CheckForInputs
from punchpipe.infrastructure.data import PUNCHData
from punchpipe.infrastructure.db import FlowEntry, FileEntry
from punchpipe.level1.destreak import DestreakFunction
from punchpipe.level1.alignment import AlignFunction
from punchpipe.level1.deficient_pixel import DeficientPixelRemovalFunction
from punchpipe.level1.despike import DespikeFunction
from punchpipe.level1.flagging import FlaggingFunction
from punchpipe.level1.psf import PSFCorrectionFunction
from punchpipe.level1.quartic_fit import QuarticFitFunction
from punchpipe.level1.vignette import VignettingCorrectionFunction
from punchpipe.level1.stray_light import StrayLightRemovalFunction


class LoadLevel0(IngestTask):
    def open(self, path):
        return PUNCHData.from_fits(path)


class OutputLevel1(OutputTask):
    def write(self, data, path):
        return data.write(path)


class Level1QueryTask(MySQLFetch):
    def __init__(self, *args, **kwargs):
        super().__init__(*args,
                         query="SELECT * FROM files WHERE state = 'finished' AND level = 0",
                         **kwargs)


class Level1InputsCheck(CheckForInputs):
    def run(self, query_result):
        output = []
        date_format = "%Y%m%dT%H%M%S"
        if query_result is not None:
            print(query_result, type(query_result))
            for result in query_result:
                now = datetime.now()
                now_time_str = datetime.strftime(now, date_format)
                date_acquired = result[6]
                date_obs = result[7]
                observation_time_str = datetime.strftime(date_obs, date_format)
                this_flow_id = f"level1_obs{observation_time_str}_run{now_time_str}"
                new_flow = FlowEntry(
                    flow_type="process level 1",
                    flow_id=this_flow_id,
                    state="queued",
                    creation_time=now,
                    priority=1,
                    call_data=json.dumps({"flow_id": this_flow_id,
                                          'input_filename': '/Users/jhughes/Desktop/repos/punchpipe/punchpipe/infrastructure/tests/L0_CL1_20211111070246.fits',
                                          'output_filename': f'/Users/jhughes/Desktop/punchpipe_output/tests/output_{random.randint(0, 100000)}.fits'})
                )
                new_file = FileEntry(
                    level=2,
                    file_type="XX",
                    observatory="X",
                    file_version=1,
                    software_version=1,
                    date_acquired=date_acquired,
                    date_observation=date_obs,
                    date_end=date_obs,
                    polarization="XX",
                    state="queued",
                    processing_flow=this_flow_id
                )
                output.append((new_flow, new_file))
        return output


destreak_task: ScienceTask = ScienceTask("destreak", DestreakFunction)
align_task: ScienceTask = ScienceTask("align", AlignFunction)
deficient_pixel_removal_task: ScienceTask = ScienceTask("deficient_pixel_removal", DeficientPixelRemovalFunction)
despike_task: ScienceTask = ScienceTask("despike", DespikeFunction)
flagging_task: ScienceTask = ScienceTask("flagging", FlaggingFunction)
psf_correction_task: ScienceTask = ScienceTask("psf_correction", PSFCorrectionFunction)
quartic_fit_task: ScienceTask = ScienceTask("quartic_fit", QuarticFitFunction)
vignetting_correction_task: ScienceTask = ScienceTask("vignetting_correction", VignettingCorrectionFunction)
stray_light_removal_task: ScienceTask = ScienceTask("stray_light_removal", StrayLightRemovalFunction)


input_filename = Parameter("input_filename")
output_filename = Parameter("output_filename")
load_level0 = LoadLevel0("load_level0")
output_level1 = OutputLevel1("output_level1")
