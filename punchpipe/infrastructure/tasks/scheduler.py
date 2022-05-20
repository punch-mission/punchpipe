from abc import abstractmethod
from typing import Optional, Tuple
import prefect
from prefect.tasks.mysql import MySQLExecute
from punchpipe.infrastructure.tasks.core import PipelineTask
from punchpipe.infrastructure.db import FlowEntry, FileEntry


class CheckForInputs(PipelineTask):
    """A generic kind of task for checking for inputs. This is inherited by a more specific task for each flow.
    """
    def __init__(self, **kwargs):
        super().__init__("check for inputs", **kwargs)

    @abstractmethod
    def run(self):
        pass


class AdvanceFiles(MySQLExecute):
    """A task that marks files as advanced to the next pipeline stage.

    This is done so that a file doesn't get queried twice.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self, query_result):
        logger = prefect.context.get("logger")
        file_ids = []
        if query_result is not None:
            for result in query_result:
                file_ids.append(result[0])
        if len(file_ids) > 0:
            self.query = f"UPDATE files SET state = 'advanced' WHERE file_id IN ({', '.join([str(i) for i in file_ids])})"
        else:  # we still have to set the query, so just set it something dumb that has no consequence
            self.query = "SELECT * FROM files WHERE file_id < 0"
        super().run()


class ScheduleFlow(MySQLExecute):
    """A task that schedules a flow.
    """
    def __init__(self, *args, pair: Optional[Tuple[FlowEntry, FileEntry]] = None, **kwargs):
        if pair is not None:
            self.flow_entry, _ = pair
        super().__init__(*args, **kwargs)

    def run(self, pair: Optional[Tuple[FlowEntry, FileEntry]] = None):
        logger = prefect.context.get("logger")

        if pair is not None:
            self.flow_entry, _ = pair
        self.query = "INSERT INTO flows (flow_id, flow_type, creation_time, priority, state, call_data)"\
                     f"VALUES ('{self.flow_entry.flow_id}', '{self.flow_entry.flow_type}', '{self.flow_entry.creation_time}'," \
                     f"{self.flow_entry.priority}, '{self.flow_entry.state}', '{self.flow_entry.call_data}');"
        logger.debug(self.query)
        super().run()
