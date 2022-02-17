from controlsegment.tasks.core import ScienceTask
from punchpipe.level1.tasks import destreak_task


def test_type_of_destreak():
    assert isinstance(destreak_task, ScienceTask)