import punchpipe
from punchpipe.infrastructure.controlsegment import ControlSegment, ControlSegmentConfiguration
from punchpipe.infrastructure.flows import LauncherFlowBuilder, SchedulerFlowBuilder
from punchpipe.infrastructure.tasks.scheduler import CheckForInputs
from credentials import db_cred
from prefect.storage import Local

if __name__ == "__main__":
    project_name = "punchpipe"
    process_flows = []


    scheduler_flows = []
    # class Level1InputsCheck(CheckForInputs):
    #     def run(self):
    #         pass
    #
    #
    # level1_inputs_check = Level1InputsCheck()
    # level1_schedule_flow = SchedulerFlowBuilder(db_cred, 1, 1, level1_inputs_check).build()

    launcher_flow = LauncherFlowBuilder(db_cred, 1).build()
    launcher_flow.storage = Local()


    control_configuration = ControlSegmentConfiguration()
    punchpipe_control_segment = ControlSegment(project_name,
                                               process_flows,
                                               scheduler_flows,
                                               launcher_flow,
                                               control_configuration,
                                               db_cred)
    punchpipe_control_segment.register_with_prefect()

