import os

from sqlalchemy import TEXT, Boolean, Column, DateTime, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class File(Base):
    __tablename__ = "files"
    file_id = Column(Integer, primary_key=True)
    level = Column(String(1), nullable=False)
    file_type = Column(String(2), nullable=False)
    observatory = Column(String(1), nullable=False)
    file_version = Column(String(16), nullable=False)
    software_version = Column(String(16), nullable=False)
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
        """Constructs the filename for this file

        Returns
        -------
        str
            properly formatted PUNCH filename
        """
        return f'PUNCH_L{self.level}_{self.file_type}{self.observatory}_{self.date_obs.strftime("%Y%m%d%H%M%S")}_v{self.file_version}.fits'

    def directory(self, root: str):
        """Constructs the directory the file should be stored in

        Parameters
        ----------
        root : str
            the root directory where the top level PUNCH file hierarchy is

        Returns
        -------
        str
            the place to write the file
        """
        return os.path.join(root, self.level, self.file_type + self.observatory, self.date_obs.strftime("%Y/%m/%d"))


class Flow(Base):
    __tablename__ = "flows"
    flow_id = Column(Integer, primary_key=True)
    flow_level = Column(String(1), nullable=False)
    flow_type = Column(String(64), nullable=False)
    flow_run_name = Column(String(64), nullable=True)
    flow_run_id = Column(String(36), nullable=True)
    state = Column(String(16), nullable=False)
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


class SciPacket(Base):
    __tablename__ = "sci_packets"
    packet_id = Column(Integer, primary_key=True)
    apid = Column(Integer, nullable=False, index=True)
    sequence_count = Column(Integer, nullable=False)
    length = Column(Integer, nullable=False)
    spacecraft_id = Column(Integer, nullable=False, index=True)
    flash_block = Column(Integer, nullable=False)
    timestamp = Column(DateTime, nullable=False, index=True)
    packet_num = Column(Integer, nullable=False)
    source_tlm_file = Column(String(128), nullable=False)  # TODO: make realistic size
    is_used = Column(Boolean)
    l0_version = Column(Integer)
    compression_settings = Column(Integer)

class EngPacket(Base):
    __tablename__ = "eng_packets"
    packet_id = Column(Integer, primary_key=True)
    apid = Column(Integer, nullable=False, index=True)
    sequence_count = Column(Integer, nullable=False)
    length = Column(Integer, nullable=False)
    spacecraft_id = Column(Integer, nullable=False, index=True)
    flash_block = Column(Integer, nullable=False)
    timestamp = Column(DateTime, nullable=False, index=True)
    packet_num = Column(Integer, nullable=False)
    source_tlm_file = Column(String(128), nullable=False)  # TODO: make realistic size
    is_used = Column(Boolean)
    l0_version = Column(Integer)


# def json_numpy_obj_hook(dct):
#     """Decodes a previously encoded numpy ndarray with proper shape and dtype.
#
#     :param dct: (dict) json encoded ndarray
#     :return: (ndarray) if input was an encoded ndarray
#     """
#     if isinstance(dct, dict) and '__ndarray__' in dct:
#         data = base64.b64decode(dct['__ndarray__'])
#         return np.frombuffer(data, dct['dtype']).reshape(dct['shape'])
#     return dct
