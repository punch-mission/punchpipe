from abc import abstractmethod
from typing import Optional, Tuple
from prefect.tasks.mysql import MySQLExecute
from prefect.utilities.tasks import defaults_from_attrs
from punchpipe.infrastructure.tasks.core import PipelineTask
from punchpipe.infrastructure.db import FlowEntry, FileEntry


class CheckForInputs(PipelineTask):
    def __init__(self, **kwargs):
        super().__init__("check for inputs", **kwargs)

    @abstractmethod
    def run(self):
        pass


class DetermineSchedule(PipelineTask):
    def __init__(self, **kwargs):
        super().__init__("determine schedule", **kwargs)

    @abstractmethod
    def run(self):
        pass


class ScheduleFile(MySQLExecute):
    def __init__(self, *args, pair: Optional[Tuple[FlowEntry, FileEntry]]= None, **kwargs):
        _, self.file_entry = pair
        super().__init__(*args, **kwargs)

    def run(self, pair: Optional[Tuple[FlowEntry, FileEntry]] = None):
        if pair is not None:
            _, self.file_entry = pair
        # TODO: also insert the processing flow for tracking purposes
        self.query = "INSERT INTO files (level, file_version, software_version, " \
                     "date_acquired, date_observation, date_end, state)"\
                     f"VALUES ({self.file_entry.level}, {self.file_entry.file_version}," \
                     f"{self.file_entry.software_version}, '{self.file_entry.date_acquired}', " \
                     f"'{self.file_entry.date_observation}'," \
                     f"'{self.file_entry.date_end}', '{self.file_entry.state}');"
        print(self.query)
        super().run()


class ScheduleFlow(MySQLExecute):
    def __init__(self, *args, pair: Optional[Tuple[FlowEntry, FileEntry]] = None, **kwargs):
        self.flow_entry, _ = pair
        super().__init__(*args, **kwargs)

    def run(self, pair: Optional[Tuple[FlowEntry, FileEntry]] = None):
        if pair is not None:
            self.flow_entry, _ = pair
        self.query = "INSERT INTO flows (flow_type, creation_time, priority, state, call_data)"\
                     f"VALUES ('{self.flow_entry.flow_type}', '{self.flow_entry.creation_time}'," \
                     f"{self.flow_entry.priority}, '{self.flow_entry.state}', '{self.flow_entry.call_data}');"
        print(self.query)
        super().run()
