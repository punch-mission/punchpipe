from typing import Optional
from abc import abstractmethod
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class FlowEntry:
    flow_type: str
    state: str
    priority: int
    creation_time: datetime
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    flow_id: Optional[int] = None
    call_data: str = ""


@dataclass
class FileEntry:
    level: int
    file_version: int
    software_version: int
    date_acquired: datetime
    date_observation: datetime
    date_end: datetime
    state: str
    file_id: Optional[int] = None
    processing_flow: Optional[int] = None



