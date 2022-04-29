import json
import random
import punchpipe
from punchpipe.infrastructure.controlsegment import ControlSegment, ControlSegmentConfiguration
from punchpipe.infrastructure.flows import LauncherFlowBuilder, SchedulerFlowBuilder, ProcessFlowBuilder
from punchpipe.infrastructure.tasks.scheduler import CheckForInputs
from punchpipe.infrastructure.db import FileEntry, FlowEntry
from punchpipe.level1.flow import level1_core_flow
from punchpipe.level1.tasks import Level1QueryTask, Level1InputsCheck
from credentials import db_cred
from prefect.storage import Local
from prefect.tasks.mysql import MySQLFetch
from datetime import datetime

if __name__ == "__main__":
    project_name = "punchpipe"
    process_flows = []
    level1_process_flow = ProcessFlowBuilder(db_cred, 1, level1_core_flow).build()
    process_flows.append(level1_process_flow)

    scheduler_flows = []
    level1_schedule_flow = SchedulerFlowBuilder(db_cred, 1, 1, Level1InputsCheck, Level1QueryTask).build()
    scheduler_flows.append(level1_schedule_flow)

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

