from typing import Optional
from abc import abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from prefect.blocks.core import Block
from pydantic import SecretStr
from sqlalchemy import Column, ForeignKey, Integer, String, create_engine, DateTime, Float
from sqlalchemy.orm import declarative_base, relationship


class MySQLCredentials(Block):
    user: Optional[str] = "localhost"
    password: Optional[SecretStr] = None 

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

Base = declarative_base()


class File(Base):
    __tablename__ = "files"
    file_id = Column(Integer, primary_key=True)
    level = Column(Integer, nullable=False)
    file_type = Column(String(2), nullable=False)
    observatory = Column(String(1), nullable=False)
    file_version = Column(Integer, nullable=False)
    software_version = Column(Integer, nullable=False)
    date_acquired = Column(DateTime, nullable=False)
    date_obs = Column(DateTime, nullable=False)
    date_end = Column(DateTime, nullable=False)
    polarization = Column(String(2), nullable=True)
    state = Column(String(64), nullable=False)
    processing_flow = Column(String(44), nullable=False)

    def __repr__(self):
        return f"File(id={self.file_id!r})"

class Flow(Base):
    __tablename__ = "flows"
    flow_id = Column(String(44), primary_key=True)
    flow_type = Column(String(64), nullable=False)
    state = Column(String(64), nullable=False)
    creation_time = Column(DateTime, nullable=False)
    start_time = Column(DateTime, nullable=True)
    end_time = Column(DateTime, nullable=True)
    priority = Column(Integer, nullable=False)
    call_data = Column(String(16000000), nullable=True)
    flow_kind = Column(Integer, nullable=True)

class FlowKind(Base):
    __tablename__ = "flow_kinds"
    flow_kind_id = Column(Integer, primary_key=True)
    flow_kind_descr = Column(String(80), nullable=True)
    fast_threshold = Column(Float, nullable=True)

class FileRelationship(Base):
    __tablename__ = "relationships"
    relationship_id = Column(Integer, primary_key=True)
    parent = Column(Integer, nullable=False)
    child = Column(Integer, nullable=False)

