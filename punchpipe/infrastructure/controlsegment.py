from __future__ import annotations
from typing import List, Dict, Any, Callable
from prefect import Flow, Client
from dataclasses import dataclass
from punchpipe.infrastructure.db import FlowEntry

MAX_SECONDS_WAITING = 10000


class ControlSegment:
    """Documentation!

    Please show up.
    """
    def __init__(self, project_name: str,
                 process_flows: List[Flow],
                 scheduler_flows: List[Flow],
                 launcher_flow: Flow,
                 control_configuration: ControlSegmentConfiguration,
                 database_credentials: DatabaseCredentials,
                 ):
        """Create a controlsegment object.

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

    @classmethod
    def create(cls, core_flows: List[Flow], configuration: Dict[str, Any]) -> DatabaseCredentials:
        """Create a control segment from the core flow definitions and a configuration dictionary

        Parameters
        ----------
        core_flows : List[Flow]
            All core flows that are expected to run, listed in order of level
        configuration: Dict[str, Any]
            Configuration object for creating a flow

        Returns
        -------
        DatabaseCredentials
            The constructed control segment with the provided core flows and configuration

        """
        # Create process flows

        # Create scheduler flows

        # Create launcher flow

        # Register with Prefect

        # Return ControlSegment object
        pass

    def _load_configuration(self, path: str) -> Dict[Any, Any]:
        pass


@dataclass
class DatabaseCredentials:
    project_name: str
    user: str
    password: str


@dataclass
class ControlSegmentConfiguration:
    max_seconds_waiting: int = 1000,
    escalated_priority: int = 100,
    max_flows_running: int = 10
