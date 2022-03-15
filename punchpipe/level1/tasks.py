from __future__ import annotations
from prefect import Parameter
from punchpipe.infrastructure.tasks.core import ScienceTask, IngestTask, OutputTask
from punchpipe.infrastructure.flows import SchedulerFlowBuilder
from punchpipe.level1.destreak import DestreakFunction
from punchpipe.infrastructure.data import PUNCHData


class LoadLevel1(IngestTask):
    def open(self, path):
        return PUNCHData.from_fits(path)


class OutputLevel2(OutputTask):
    def write(self, data, path):
        data.write(path, "wfi-starfield")


destreak_task: ScienceTask = ScienceTask("destreak", DestreakFunction)

input_filename = Parameter("input_filename")
output_filename = Parameter("output_filename")
load_level1 = LoadLevel1("load_level1")
output_level2 = OutputLevel2("output_level2")