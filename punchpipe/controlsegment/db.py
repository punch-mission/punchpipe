from typing import Optional
from prefect.blocks.core import Block
from pydantic import SecretStr
from sqlalchemy import Column, ForeignKey, Integer, String, create_engine, DateTime, Float, TEXT
from sqlalchemy.orm import declarative_base, relationship


class MySQLCredentials(Block):
    user: Optional[str] = "localhost"
    password: Optional[SecretStr] = None


Base = declarative_base()


class File(Base):
    __tablename__ = "files"
    file_id = Column(Integer, primary_key=True)
    level = Column(Integer, nullable=False)
    file_type = Column(String(2), nullable=False)
    observatory = Column(String(1), nullable=False)
    file_version = Column(Integer, nullable=False)
    software_version = Column(Integer, nullable=False)
    date_created = Column(DateTime, nullable=True)
    date_obs = Column(DateTime, nullable=False)
    date_beg = Column(DateTime, nullable=True)
    date_end = Column(DateTime, nullable=True)
    polarization = Column(String(2), nullable=True)
    state = Column(String(64), nullable=False)
    processing_flow = Column(Integer, nullable=True)

    def __repr__(self):
        return f"File(id={self.file_id!r})"

    def filename(self) -> str:
        return f'PUNCH_L{self.level}_{self.file_type}{self.observatory}_{self.date_obs.strftime("%Y%m%d%H%M%S")}.fits'

class Flow(Base):
    __tablename__ = "flows"
    flow_id = Column(Integer, primary_key=True)
    flow_type = Column(String(64), nullable=False)
    flow_run = Column(String(64), nullable=True)
    state = Column(String(64), nullable=False)
    creation_time = Column(DateTime, nullable=False)
    start_time = Column(DateTime, nullable=True)
    end_time = Column(DateTime, nullable=True)
    priority = Column(Integer, nullable=False)
    call_data = Column(TEXT, nullable=True)


class FileRelationship(Base):
    __tablename__ = "relationships"
    relationship_id = Column(Integer, primary_key=True)
    parent = Column(Integer, nullable=False)
    child = Column(Integer, nullable=False)

