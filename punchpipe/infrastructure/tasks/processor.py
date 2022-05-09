from datetime import datetime
from typing import Optional
import prefect
from prefect.tasks.mysql import MySQLExecute
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
    """Update the database that a file has been created."""
    def __init__(self, *args,  meta_data: Optional[dict] = None, flow_id: Optional[str] = None, **kwargs):
        self.meta_data = meta_data
        self.flow_id = flow_id
        super().__init__(*args, **kwargs)

    def run(self, meta_data: Optional[dict] = None, flow_id: Optional[str] = None):
        logger = prefect.context.get("logger")
        meta_data['processing_flow'] = flow_id
        self.query = f"INSERT INTO files (level, file_type, observatory, file_version, software_version, " \
                     f"date_acquired, date_obs, date_end, polarization, state, processing_flow) VALUES(" \
                     f"'{meta_data['level']}', '{meta_data['file_type']}', '{meta_data['observatory']}', " \
                     f"{meta_data['file_version']}, {meta_data['software_version']}, '{meta_data['date_acquired']}'," \
                     f"'{meta_data['date_obs']}', '{meta_data['date_end']}', '{meta_data['polarization']}', '{meta_data['state']}', " \
                     f"'{meta_data['processing_flow']}');"
        logger.debug("query=" + self.query)
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
