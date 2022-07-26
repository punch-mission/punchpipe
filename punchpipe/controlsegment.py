from __future__ import annotations
from typing import List, Dict, Any
from prefect import Flow, Client
from dataclasses import dataclass


MAX_SECONDS_WAITING = 10000  # TODO: remove and make come from controlsegment configuration


class ControlSegment:
    """The PUNCH control segment. This module handles initializing the control segment that is then run through Prefect.
    """
    def __init__(self, project_name: str,
                 process_flows: List[Flow],
                 scheduler_flows: List[Flow],
                 launcher_flow: Flow,
                 control_configuration: ControlSegmentConfiguration,
                 database_credentials: DatabaseCredentials,
                 ):
        """Create a ContrlSegment object.

        Parameters
        ----------
        project_name: str
            The name of the project to be used in Prefect
        process_flows: List[Flow]
            List of all process flows for scientific reduction levels
        scheduler_flows: List[Flow]
            List of all scheduler flows to handle scheduling flows
        launcher_flow: Flow
            The launcher flow that handles fetching from the database what should run
        control_configuration : ControlSegmentConfiguration
            Configuration parameters for control segment
        database_credentials: DatabaseCredentials
            MySQL database credentials
        """
        self.project_name = project_name

        self.process_flows = process_flows
        self.scheduler_flows = scheduler_flows
        self.launcher_flow = launcher_flow

        self.control_configuration = control_configuration

        self.database_credentials = database_credentials

    def register_with_prefect(self) -> None:
        """Registers a flow with Prefect.

        Notes
        ------
        This will create a project with the specified name as needed. It will also register every flow, not just the
        process flows.

        Returns
        -------
        None
        """
        client = Client()
        client.create_project(project_name=self.project_name)

        for flow in [*self.process_flows, *self.scheduler_flows, self.launcher_flow]:
            flow.register(project_name=self.project_name)


@dataclass
class DatabaseCredentials:
    """Credentials for a database, i.e. the database name (called the project_name here), the user, and the password.
    """
    project_name: str
    user: str
    password: str


@dataclass
class ControlSegmentConfiguration:
    """All the configuration parameters for the controlsegment.
    """
    max_seconds_waiting: int = 1000  # how long a flow is allowed to be queued before priority gets escalated
    escalated_priority: int = 100  # the priority a flow gets escalated after waiting `max_seconds_waiting`
    max_flows_running: int = 10  # the maximum number of concurrent flows allowed to be running, any more must wait
