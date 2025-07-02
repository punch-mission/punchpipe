import os

from sqlalchemy import TEXT, Boolean, Column, Float, Index, Integer, String
from sqlalchemy.dialects.mysql import DATETIME
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class File(Base):
    __tablename__ = "files"
    file_id = Column(Integer, primary_key=True)
    level = Column(String(1), nullable=False)
    file_type = Column(String(2), nullable=False)
    observatory = Column(String(1), nullable=False)
    file_version = Column(String(16), nullable=False)
    software_version = Column(String(35), nullable=False)
    date_created = Column(DATETIME(fsp=6), nullable=True)
    date_obs = Column(DATETIME(fsp=6), nullable=False)
    date_beg = Column(DATETIME(fsp=6), nullable=True)
    date_end = Column(DATETIME(fsp=6), nullable=True)
    polarization = Column(String(2), nullable=True)
    state = Column(String(64), nullable=False)
    processing_flow = Column(Integer, nullable=True)

    def summary(self):
        return (
            f"File {self.file_id}, level {self.level}, type {self.file_type}{self.observatory}, state {self.state}\n"
            f"File version {self.file_version}, from software {self.software_version}\n"
            f"date_obs {self.date_obs}, created {self.date_created}\n"
        )

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


Index("date_obs_index", File.date_obs, mysql_using="btree", mariadb_using="btree")
Index("observatory_index", File.observatory, mysql_using="hash", mariadb_using="hash")
Index("file_type_index", File.file_type, mysql_using="hash", mariadb_using="hash")


class Flow(Base):
    __tablename__ = "flows"
    flow_id = Column(Integer, primary_key=True)
    flow_level = Column(String(1), nullable=False)
    flow_type = Column(String(64), nullable=False)
    flow_run_name = Column(String(64), nullable=True)
    flow_run_id = Column(String(36), nullable=True)
    state = Column(String(16), nullable=False)
    creation_time = Column(DATETIME(fsp=6), nullable=False)
    launch_time = Column(DATETIME(fsp=6), nullable=True)
    start_time = Column(DATETIME(fsp=6), nullable=True)
    end_time = Column(DATETIME(fsp=6), nullable=True)
    priority = Column(Integer, nullable=False)
    call_data = Column(TEXT, nullable=True)

    def __repr__(self):
        return f"Flow(id={self.flow_id!r})"


class FileRelationship(Base):
    __tablename__ = "relationships"
    relationship_id = Column(Integer, primary_key=True)
    parent = Column(Integer, nullable=False)
    child = Column(Integer, nullable=False)

class TLMFiles(Base):
    __tablename__ = "tlm_files"
    tlm_id = Column(Integer, primary_key=True)
    path = Column(String(128), nullable=False)
    successful = Column(Boolean, nullable=False)
    num_attempts = Column(Integer, nullable=False)
    last_attempt = Column(DATETIME(fsp=6), nullable=True)

class Health(Base):
    __tablename__ = "health"
    health_id = Column(Integer, primary_key=True)
    datetime = Column(DATETIME(fsp=6), nullable=False)
    cpu_usage = Column(Float, nullable=False)
    memory_usage = Column(Float, nullable=False)
    memory_percentage = Column(Float, nullable=False)
    disk_usage = Column(Float, nullable=False)
    disk_percentage = Column(Float, nullable=False)
    num_pids = Column(Integer, nullable=False)


class PacketHistory(Base):
    __tablename__ = "packet_history"
    id = Column(Integer, primary_key=True)
    datetime = Column(DATETIME(fsp=6), nullable=False)
    num_images_succeeded = Column(Integer, nullable=False)
    num_images_failed = Column(Integer, nullable=False)

class SCI_XFI(Base):
    __tablename__ = "sci_xfi"
    id = Column(Integer, primary_key=True)
    tlm_id = Column(Integer)
    spacecraft_id = Column(Integer, nullable=False)
    packet_index = Column(Integer, nullable=False)
    ccsds_sequence_count = Column(Integer, nullable=False)
    ccsds_packet_length = Column(Integer, nullable=False)
    timestamp = Column(DATETIME(fsp=6), nullable=False, index=True)
    is_used = Column(Boolean, nullable=False)
    num_attempts = Column(Integer, nullable=True)
    last_attempt = Column(DATETIME(fsp=6), nullable=True)
    last_skip_reason = Column(TEXT, nullable=True)
    flash_block = Column(Integer, nullable=False)
    compression_settings = Column(Integer, nullable=False)
    acquisition_settings = Column(Integer, nullable=False)
    packet_group = Column(Integer, nullable=False)

class ENG_CEB(Base):
    __tablename__ = "eng_ceb"
    id = Column(Integer, primary_key=True)
    tlm_id = Column(Integer)
    spacecraft_id = Column(Integer, nullable=False)
    packet_index = Column(Integer, nullable=False)
    ccsds_sequence_count = Column(Integer, nullable=False)
    ccsds_packet_length = Column(Integer, nullable=False)
    timestamp = Column(DATETIME(fsp=6), nullable=False, index=True)

class ENG_PFW(Base):
    __tablename__ = "eng_pfw"
    id = Column(Integer, primary_key=True)
    tlm_id = Column(Integer)
    spacecraft_id = Column(Integer, nullable=False)
    packet_index = Column(Integer, nullable=False)
    ccsds_sequence_count = Column(Integer, nullable=False)
    ccsds_packet_length = Column(Integer, nullable=False)
    timestamp = Column(DATETIME(fsp=6), nullable=False, index=True)

class ENG_XACT(Base):
    __tablename__ = "eng_xact"
    id = Column(Integer, primary_key=True)
    tlm_id = Column(Integer)
    spacecraft_id = Column(Integer, nullable=False)
    packet_index = Column(Integer, nullable=False)
    ccsds_sequence_count = Column(Integer, nullable=False)
    ccsds_packet_length = Column(Integer, nullable=False)
    timestamp = Column(DATETIME(fsp=6), nullable=False, index=True)

class ENG_LZ(Base):
    __tablename__ = "eng_lz"
    id = Column(Integer, primary_key=True)
    tlm_id = Column(Integer)
    spacecraft_id = Column(Integer, nullable=False)
    packet_index = Column(Integer, nullable=False)
    ccsds_sequence_count = Column(Integer, nullable=False)
    ccsds_packet_length = Column(Integer, nullable=False)
    timestamp = Column(DATETIME(fsp=6), nullable=False, index=True)

class ENG_LED(Base):
    __tablename__ = "eng_led"
    id = Column(Integer, primary_key=True)
    tlm_id = Column(Integer)
    spacecraft_id = Column(Integer, nullable=False)
    packet_index = Column(Integer, nullable=False)
    ccsds_sequence_count = Column(Integer, nullable=False)
    ccsds_packet_length = Column(Integer, nullable=False)
    timestamp = Column(DATETIME(fsp=6), nullable=False, index=True)
    led_start_time = Column(DATETIME(fsp=6), nullable=False, index=True)
    led_end_time = Column(DATETIME(fsp=6), nullable=False, index=True)

PACKETNAME2SQL = {'SCI_XFI': SCI_XFI,
                  'ENG_CEB': ENG_CEB,
                  'ENG_PFW': ENG_PFW,
                  'ENG_XACT': ENG_XACT,
                  'ENG_LED': ENG_LED,
                  "ENG_LZ": ENG_LZ}

def get_closest_file(f_target: File, f_others: list[File]) -> File:
    return min(f_others, key=lambda o: abs((f_target.date_obs - o.date_obs).total_seconds()))


def get_closest_before_file(f_target: File, f_others: list[File]) -> File:
    return get_closest_file(f_target, [o for o in f_others if f_target.date_obs >= o.date_obs])


def get_closest_after_file(f_target: File, f_others: list[File]) -> File:
    return get_closest_file(f_target, [o for o in f_others if f_target.date_obs <= o.date_obs])
