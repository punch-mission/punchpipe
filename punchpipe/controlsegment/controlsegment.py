from __future__ import annotations
from typing import List, Dict, Any
from prefect import Flow
from dataclasses import dataclass


MAX_SECONDS_WAITING = 10000  # TODO: remove and make come from controlsegment configuration


@dataclass
class PipelineConfiguration:
    """All the configuration parameters for the controlsegment.
    """
    max_seconds_waiting: int = 1000  # how long a flow is allowed to be queued before priority gets escalated
    escalated_priority: int = 100  # the priority a flow gets escalated after waiting `max_seconds_waiting`
    max_flows_running: int = 10  # the maximum number of concurrent flows allowed to be running, others must wait
    flow_configs: dict = ()

    def load_config(self, path):
        pass
