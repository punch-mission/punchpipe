from datetime import datetime
from typing import Optional
import prefect
from prefect.tasks.mysql import MySQLFetch, MySQLExecute
from prefect.utilities.tasks import defaults_from_attrs
from punchpipe.infrastructure.db import FlowEntry


class MarkFlowAsRunning(MySQLExecute):
    """A task that simply marks a flow, specified by its ID, as running.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self, flow_id: str):
        logger = prefect.context.get("logger")
        self.query = f"UPDATE flows SET state = 'running' WHERE flow_id = '{flow_id}'; "
        logger.debug("query is " + self.query)
        super().run()


class MarkFlowStartTime(MySQLExecute):
    """A task that simply marks when a flow, specified by its ID, begins running, i.e. now.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self, flow_id: str):
        self.query = f"UPDATE flows SET start_time = '{datetime.now()}' WHERE flow_id = '{flow_id}';"
        super().run()


class CreateFileDatabaseEntry(MySQLExecute):
    """Not sure!"""
    def __init__(self, *args, flow_entry: Optional[FlowEntry] = None, **kwargs):
        self.flow_entry = flow_entry
        super().__init__(*args, **kwargs)

    @defaults_from_attrs('flow_entry')
    def run(self, flow_entry: Optional[FlowEntry] = None):
        self.query = f"SOMETHING"  # TODO: figure out
        super().run()


class MarkFlowAsEnded(MySQLExecute):
    """A task a flow that updates the state of a flow, specified by its ID, as ended."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self, flow_id: str):
        self.query = f"UPDATE flows SET state = 'ended' WHERE flow_id = '{flow_id}'; "
        super().run()


class MarkFlowEndTime(MySQLExecute):
    """A task that updates the end time of a flow, specified by its ID, to now."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self, flow_id: str):
        self.query = f"UPDATE flows SET end_time = '{datetime.now()}' WHERE flow_id = '{flow_id}';"
        super().run()


class MarkFlowAsFailed(MySQLExecute):
    """A task that marks a flow, specified by its ID, as failed.

    This task is used a final check for processor flows. It links to all prior tasks and is triggered by failure of any
    prior task. Thus, it does a cleanup of the database to make sure flows aren't left marked as running when they fail.
    """
    def __init__(self, *args, flow_entry: Optional[FlowEntry] = None, **kwargs):
        self.flow_entry = flow_entry
        super().__init__(*args, **kwargs)

    def run(self, flow_id: str):
        self.query = f"UPDATE flows SET state = 'failed' WHERE flow_id = '{flow_id}'; "
        super().run()
