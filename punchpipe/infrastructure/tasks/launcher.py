"""Launcher flow specific tasks.
"""
import json
from typing import List, Optional, Tuple
from prefect.tasks.prefect import create_flow_run
from prefect.tasks.mysql import MySQLExecute, MySQLFetch
from punchpipe.infrastructure.tasks.core import PipelineTask
from punchpipe.infrastructure.controlsegment import MAX_SECONDS_WAITING


class GatherQueuedFlows(MySQLFetch):
    def __init__(self, *args, **kwargs):
        super().__init__(*args,
                         query="SELECT * FROM flows WHERE state = 'queued' ORDER BY priority DESC;",
                         **kwargs)


class CountRunningFlows(MySQLFetch):
    def __init__(self, *args, **kwargs):
        super().__init__(*args,
                         query="SELECT COUNT(*) FROM flows WHERE state = 'running';",
                         **kwargs)


class EscalateLongWaitingFlows(MySQLExecute):
    def __init__(self, *args, **kwargs):
        super().__init__(*args,
                         query=f"UPDATE flows f1 LEFT JOIN flows f2 ON f1.flow_id = f2.flow_id SET f1.priority = 100 "
                               f"WHERE TIMESTAMPDIFF(SECOND, f1.creation_time, now()) > {MAX_SECONDS_WAITING} "
                               f"AND f1.state = 'queued';",
                         # TODO: update this query to use customizable flow escalation parameter
                         **kwargs)


class FilterForLaunchableFlows(PipelineTask):
    def __init__(self,  **kwargs):
        super().__init__("filter for launchable flows", **kwargs)

    def run(self, running_flow_count: Tuple[int], priority_sorted_queued_flows: List, max_flows_running: int = 10):
        number_to_launch = max_flows_running - running_flow_count[0]
        if number_to_launch > 0:
            if priority_sorted_queued_flows:  # If there are no flows, it'll be None
                return priority_sorted_queued_flows[:number_to_launch]
            else:
                return []
        else:
            return []


class LaunchFlow(PipelineTask):
    def __init__(self, **kwargs):
        super().__init__("launch flow", **kwargs)

    def run(self, flow_entry):
        create_flow_run.run(flow_name=flow_entry[1], parameters=json.loads(flow_entry[-1]))

