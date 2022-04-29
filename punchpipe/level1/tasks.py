from __future__ import annotations
from prefect import Parameter
from punchpipe.infrastructure.tasks.core import ScienceTask, IngestTask, OutputTask
from punchpipe.infrastructure.flows import SchedulerFlowBuilder
from punchpipe.level1.destreak import DestreakFunction
from punchpipe.level1.alignment import AlignFunction
from punchpipe.level1.deficient_pixel import DeficientPixelRemovalFunction
from punchpipe.level1.despike import DespikeFunction
from punchpipe.level1.flagging import FlaggingFunction
from punchpipe.level1.psf import PSFCorrectionFunction
from punchpipe.level1.quartic_fit import QuarticFitFunction
from punchpipe.level1.vignette import VignettingCorrectionFunction
from punchpipe.level1.stray_light import StrayLightRemovalFunction
from punchpipe.infrastructure.data import PUNCHData


class LoadLevel0(IngestTask):
    def open(self, path):
        return PUNCHData.from_fits(path)


class OutputLevel1(OutputTask):
    def write(self, data, path):
        return data.write(path)


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
