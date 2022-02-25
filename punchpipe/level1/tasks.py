from __future__ import annotations
from punchpipe.infrastructure.tasks.core import ScienceTask
from punchpipe.level1.destreak import DestreakFunction


destreak_task: ScienceTask = ScienceTask("destreak", DestreakFunction)
