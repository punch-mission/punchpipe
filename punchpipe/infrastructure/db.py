from typing import Optional
from abc import abstractmethod
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class FlowEntry:
    """A representation of an entry from the flows table in the database.
    """
    flow_type: str
    state: str
    priority: int
    creation_time: datetime
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    flow_id: Optional[str] = None
    call_data: str = ""


@dataclass
class FileEntry:
    """A representation of an entry from the files table in the database.
    """
    level: int
    file_type: str
    observatory: str
    file_version: int
    software_version: int
    date_acquired: datetime
    date_observation: datetime
    date_end: datetime
    polarization: str
    state: str
    file_id: Optional[int] = None
    processing_flow: Optional[str] = None



