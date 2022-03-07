from punchpipe.infrastructure.controlsegment import ControlSegment, ControlSegmentConfiguration
from punchpipe.infrastructure.flows import LauncherFlowBuilder
from credentials import db_cred


project_name = "punchpipe"
process_flows = []
scheduler_flows = []
launcher_flow = LauncherFlowBuilder(db_cred, 1).build()
control_configuration = ControlSegmentConfiguration()
punchpipe_control_segment = ControlSegment(project_name,
                                           process_flows,
                                           scheduler_flows,
                                           launcher_flow,
                                           control_configuration,
                                           db_cred)
punchpipe_control_segment.register_with_prefect()

