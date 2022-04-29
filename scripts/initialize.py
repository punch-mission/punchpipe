import json
import random
import punchpipe
from punchpipe.infrastructure.controlsegment import ControlSegment, ControlSegmentConfiguration
from punchpipe.infrastructure.flows import LauncherFlowBuilder, SchedulerFlowBuilder, ProcessFlowBuilder
from punchpipe.infrastructure.tasks.scheduler import CheckForInputs
from punchpipe.infrastructure.db import FileEntry, FlowEntry
from punchpipe.level1.flow import level1_core_flow
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


    level1_inputs_check = Level1InputsCheck()
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

