from datetime import datetime
from typing import Optional
from prefect.tasks.mysql import MySQLFetch, MySQLExecute
from prefect.utilities.tasks import defaults_from_attrs
from punchpipe.infrastructure.db import FlowEntry


class MarkFlowAsRunning(MySQLExecute):
    def __init__(self, *args, flow_entry: Optional[FlowEntry] = None, **kwargs):
        self.flow_entry = flow_entry
        super().__init__(*args, **kwargs)

    @defaults_from_attrs('flow_entry')
    def run(self, flow_entry: Optional[FlowEntry] = None):
        self.query = f"UPDATE flows SET state = 'running' WHERE flow_id = {flow_entry.flow_id}; "
        print(self.query)
        super().run()


class MarkFlowStartTime(MySQLExecute):
    def __init__(self, *args, flow_entry: Optional[FlowEntry] = None, **kwargs):
        self.flow_entry = flow_entry
        super().__init__(*args, **kwargs)

    @defaults_from_attrs('flow_entry')
    def run(self, flow_entry: Optional[FlowEntry] = None):
        self.query = f"UPDATE flows SET start_time = '{datetime.now()}' WHERE flow_id = {flow_entry.flow_id};"
        print(self.query)
        super().run()


class CreateFileDatabaseEntry(MySQLExecute):
    def __init__(self, *args, flow_entry: Optional[FlowEntry] = None, **kwargs):
        self.flow_entry = flow_entry
        super().__init__(*args, **kwargs)

    @defaults_from_attrs('flow_entry')
    def run(self, flow_entry: Optional[FlowEntry] = None):
        self.query = f"SOMETHING"  # TODO: figure out
        print(self.query)
        super().run()


class MarkFlowAsEnded(MySQLExecute):
    def __init__(self, *args, flow_entry: Optional[FlowEntry] = None, **kwargs):
        self.flow_entry = flow_entry
        super().__init__(*args, **kwargs)

    @defaults_from_attrs('flow_entry')
    def run(self, flow_entry: Optional[FlowEntry] = None):
        self.query = f"UPDATE flows SET state = 'ended' WHERE flow_id = {flow_entry.flow_id}; "
        print(self.query)
        super().run()


class MarkFlowEndTime(MySQLExecute):
    def __init__(self, *args, flow_entry: Optional[FlowEntry] = None, **kwargs):
        self.flow_entry = flow_entry
        super().__init__(*args, **kwargs)

    @defaults_from_attrs('flow_entry')
    def run(self, flow_entry: Optional[FlowEntry] = None):
        self.query = f"UPDATE flows SET end_time = '{datetime.now()}' WHERE flow_id = {flow_entry.flow_id};"
        print(self.query)
        super().run()
