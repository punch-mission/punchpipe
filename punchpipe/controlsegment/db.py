import os

from sqlalchemy import TEXT, Boolean, Column, DateTime, Float, Integer, String
from sqlalchemy.orm import declarative_base

from punchpipe.error import MissingCCSDSDataError

Base = declarative_base()


class File(Base):
    __tablename__ = "files"
    file_id = Column(Integer, primary_key=True)
    level = Column(String(1), nullable=False)
    file_type = Column(String(2), nullable=False)
    observatory = Column(String(1), nullable=False)
    file_version = Column(String(16), nullable=False)
    software_version = Column(String(35), nullable=False)
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
    source_tlm_file = Column(Integer, nullable=False)
    is_used = Column(Boolean)
    l0_version = Column(Integer)
    compression_settings = Column(Integer)

class EngXACTPacket(Base):
    __tablename__ = "eng_xact"
    packet_id = Column(Integer, primary_key=True)
    apid = Column(Integer, nullable=False, index=True)
    sequence_count = Column(Integer, nullable=False)
    length = Column(Integer, nullable=False)
    spacecraft_id = Column(Integer, nullable=False, index=True)
    flash_block = Column(Integer, nullable=False)
    timestamp = Column(DateTime, nullable=False, index=True)
    packet_num = Column(Integer, nullable=False)
    source_tlm_file = Column(Integer, nullable=False)

    ATT_DET_Q_BODY_WRT_ECI1	= Column(Float, nullable=False) # Attitude Quaternion
    ATT_DET_Q_BODY_WRT_ECI2	= Column(Float, nullable=False) # Attitude Quaternion
    ATT_DET_Q_BODY_WRT_ECI3	= Column(Float, nullable=False) # Attitude Quaternion
    ATT_DET_Q_BODY_WRT_ECI4	= Column(Float, nullable=False) # Attitude Quaternion

    ATT_DET_RESIDUAL1 = Column(Float, nullable=False) #	Attitude Filter Residual
    ATT_DET_RESIDUAL2 = Column(Float, nullable=False) # Attitude Filter Residual
    ATT_DET_RESIDUAL3 = Column(Float, nullable=False) # Attitude Filter Residual

    REFS_POSITION_WRT_ECI1 = Column(Float, nullable=False) # Orbit Position ECI
    REFS_POSITION_WRT_ECI2 = Column(Float, nullable=False) # Orbit Position ECI
    REFS_POSITION_WRT_ECI3 = Column(Float, nullable=False) # Orbit Position ECI

    REFS_VELOCITY_WRT_ECI1 = Column(Float, nullable=False) # Orbit Velocity ECI
    REFS_VELOCITY_WRT_ECI2 = Column(Float, nullable=False) # Orbit Velocity ECI
    REFS_VELOCITY_WRT_ECI3 = Column(Float, nullable=False) # Orbit Velocity ECI

    ATT_CMD_CMD_Q_BODY_WRT_ECI1 = Column(Float, nullable=False) # Commanded Att Quaternion
    ATT_CMD_CMD_Q_BODY_WRT_ECI2 = Column(Float, nullable=False) # Commanded Att Quaternion
    ATT_CMD_CMD_Q_BODY_WRT_ECI3 = Column(Float, nullable=False) # Commanded Att Quaternion
    ATT_CMD_CMD_Q_BODY_WRT_ECI4	= Column(Float, nullable=False) # Commanded Att Quaternion

class ENGPFWPacket(Base):
    __tablename__ = "eng_pfw"
    packet_id = Column(Integer, primary_key=True)
    apid = Column(Integer, nullable=False, index=True)
    sequence_count = Column(Integer, nullable=False)
    length = Column(Integer, nullable=False)
    spacecraft_id = Column(Integer, nullable=False, index=True)
    flash_block = Column(Integer, nullable=False)
    timestamp = Column(DateTime, nullable=False, index=True)
    packet_num = Column(Integer, nullable=False)
    source_tlm_file = Column(Integer, nullable=False)

    PFW_STATUS =Column(Integer, nullable=False)  # Current PFW Status (0 - no error, else error)
    STEP_CALC = Column(Integer, nullable=False) # Calculated step (0-1199)
    LAST_CMD_N_STEPS = Column(Integer, nullable=False) # Commanded number of steps (1-1199)
    HOME_POSITION_OVRD = Column(Integer, nullable=False) # HOME Position OVERRIDE
    POSITION_CURR = Column(Integer, nullable=False) # Current position (1-5, 0 - manual stepping)
    POSITION_CMD = Column(Integer, nullable=False) # Commanded position (1-5, 0 - manual stepping)
    RESOLVER_POS_RAW = Column(Integer, nullable=False) # Resolver position - raw resolver counts (0-65000)
    RESOLVER_POS_CORR = Column(Integer, nullable=False) # Resolver position - error correction applied (0-65000)
    RESOLVER_READ_CNT = Column(Integer, nullable=False) # Accumulative # of resolver reads (resets on boot)
    LAST_MOVE_N_STEPS = Column(Integer, nullable=False)# Number of steps on last move (1-1199)
    LAST_MOVE_EXECUTION_TIME = Column(Float, nullable=False) # Current move execution time
    LIFETIME_STEPS_TAKEN = Column(Integer, nullable=False) # Lifetime accumulative number of steps taken
    LIFETIME_EXECUTION_TIME	= Column(Float, nullable=False) # Lifetime accumulative execution time
    FSM_CTRL_STATE = Column(Integer, nullable=False) # Controller FSM State
    READ_SUB_STATE = Column(Integer, nullable=False) # READ Sub-FSM State
    MOVE_SUB_STATE = Column(Integer, nullable=False) # MOVE Sub-FSM State
    HOME_SUB_STATE = Column(Integer, nullable=False) # HOME Sub-FSM State
    HOME_POSITION = Column(Integer, nullable=False) # Home Position (1-5)
    RESOLVER_SELECT = Column(Integer, nullable=False) # Resolver Select
    RESOLVER_TOLERANCE_HOME = Column(Integer, nullable=False) # Resolver Tolerance
    RESOLVER_TOLERANCE_CURR = Column(Integer, nullable=False) # Resolver Tolerance
    STEPPER_SELECT= Column(Integer, nullable=False) # Stepper Motor Select
    STEPPER_RATE_DELAY = Column(Integer, nullable=False) # Stepper Motor Rate Delay
    STEPPER_RATE = Column(Float, nullable=False) # Stepper Motor Rate
    SHORT_MOVE_SETTLING_TIME_MS	= Column(Integer, nullable=False) # Short Move(1-4 steps) Settling time before reading resolver
    LONG_MOVE_SETTLING_TIME_MS = Column(Integer, nullable=False) # Long Move(5-1199 steps) Setting time before reading resolver
    PRIMARY_STEP_OFFSET_1 = Column(Integer, nullable=False) # Primary Step Offset 1
    PRIMARY_STEP_OFFSET_2 = Column(Integer, nullable=False) # Short Move(1-4 steps) Delay before reading resolver
    PRIMARY_STEP_OFFSET_3 = Column(Integer, nullable=False) # Primary Step Offset 3
    PRIMARY_STEP_OFFSET_4 = Column(Integer, nullable=False) # Primary Step Offset 4
    PRIMARY_STEP_OFFSET_5 = Column(Integer, nullable=False) # Primary Step Offset 5
    REDUNDANT_STEP_OFFSET_1 = Column(Integer, nullable=False) # Redundant Step Offset 1
    REDUNDANT_STEP_OFFSET_2 = Column(Integer, nullable=False) # Redundant Step Offset 2
    REDUNDANT_STEP_OFFSET_3 = Column(Integer, nullable=False) # Redundant Step Offset 3
    REDUNDANT_STEP_OFFSET_4 = Column(Integer, nullable=False) # Redundant Step Offset 4
    REDUNDANT_STEP_OFFSET_5 = Column(Integer, nullable=False) # Redundant Step Offset 5
    PRIMARY_RESOLVER_POSITION_1 = Column(Integer, nullable=False) # Primary Resolver Position 1
    PRIMARY_RESOLVER_POSITION_2 = Column(Integer, nullable=False) # Primary Resolver Position 2
    PRIMARY_RESOLVER_POSITION_3 = Column(Integer, nullable=False) # Primary Resolver Position 3
    PRIMARY_RESOLVER_POSITION_4 = Column(Integer, nullable=False) # Primary Resolver Position 4
    PRIMARY_RESOLVER_POSITION_5 = Column(Integer, nullable=False) # Primary Resolver Position 5
    REDUNDANT_RESOLVER_POSITION_1 = Column(Integer, nullable=False) # Redundant Resolver Position 1
    REDUNDANT_RESOLVER_POSITION_2 = Column(Integer, nullable=False) # Redundant Resolver Position 2
    REDUNDANT_RESOLVER_POSITION_3 = Column(Integer, nullable=False) # Redundant Resolver Position 3
    REDUNDANT_RESOLVER_POSITION_4 = Column(Integer, nullable=False) # Redundant Resolver Position 4
    REDUNDANT_RESOLVER_POSITION_5 = Column(Integer, nullable=False) # Redundant Resolver Position 5


class TLMFiles(Base):
    __tablename__ = "tlm_files"
    tlm_id = Column(Integer, primary_key=True)
    path = Column(String(128), nullable=False)
    is_processed = Column(Boolean, nullable=False)

class Health(Base):
    __tablename__ = "health"
    health_id = Column(Integer, primary_key=True)
    datetime = Column(DateTime, nullable=False)
    cpu_usage = Column(Float, nullable=False)
    memory_usage = Column(Float, nullable=False)
    memory_percentage = Column(Float, nullable=False)
    disk_usage = Column(Float, nullable=False)
    disk_percentage = Column(Float, nullable=False)
    num_pids = Column(Integer, nullable=False)


def get_closest_eng_packets(table, timestamp, spacecraft_id):
    # find the closest events which are greater/less than the timestamp
    gt_event = table.query.filter(table.spacecraft_id == spacecraft_id).filter(table.timestamp > timestamp).order_by(table.timestamp.asc()).first()
    lt_event = table.query.filter(table.spacecraft_id == spacecraft_id).filter(table.timestamp < timestamp).order_by(table.timestamp.desc()).first()

    if gt_event is None and lt_event is None:
        msg = "Could not find packet near that time."
        raise MissingCCSDSDataError(msg)
    elif gt_event is not None and lt_event is None:
        lt_event = gt_event
    elif gt_event is None and lt_event is not None:
        gt_event = lt_event

    return lt_event, gt_event
