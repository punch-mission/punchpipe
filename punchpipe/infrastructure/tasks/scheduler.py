import logging
from abc import abstractmethod
from typing import Optional, Tuple
import prefect
from prefect.tasks.mysql import MySQLExecute
from prefect.utilities.tasks import defaults_from_attrs
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


class DetermineSchedule(PipelineTask):
    """wut?
    """
    def __init__(self, **kwargs):
        super().__init__("determine schedule", **kwargs)

    @abstractmethod
    def run(self):
        pass


class ScheduleFile(MySQLExecute):
    """A task that schedules a file.

    This is done at the same time that a flow is scheduled. It creates a placeholder for the new file that is going
    to be created so that it doesn't get queried twice.
    """
    def __init__(self, *args, pair: Optional[Tuple[FlowEntry, FileEntry]]= None, **kwargs):
        if pair is not None:
            _, self.file_entry = pair
        super().__init__(*args, **kwargs)

    def run(self, pair: Optional[Tuple[FlowEntry, FileEntry]] = None):
        logger = prefect.context.get("logger")

        if pair is not None:
            _, self.file_entry = pair
        # TODO: also insert the processing flow for tracking purposes
        self.query = "INSERT INTO files (level, file_type, observatory, file_version, software_version, " \
                     "date_acquired, date_obs, date_end, polarization, state, processing_flow)"\
                     f"VALUES ({self.file_entry.level}, {self.file_entry.file_type}, {self.file_entry.observatory}, {self.file_entry.file_version}," \
                     f"{self.file_entry.software_version}, '{self.file_entry.date_acquired}', " \
                     f"'{self.file_entry.date_observation}'," \
                     f"'{self.file_entry.date_end}', '{self.file_entry.polarization}', '{self.file_entry.state}', '{self.file_entry.processing_flow}');"
        logger.debug(self.query)
        print(self.query)
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
        print(self.query)
        logger.debug(self.query)
        super().run()
