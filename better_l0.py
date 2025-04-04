import io
from glob import glob
from datetime import datetime

import ccsdspy
import pandas as pd
import sqlalchemy
from ccsdspy import PacketArray, PacketField, converters
from ccsdspy.utils import split_by_apid
from prefect_sqlalchemy import SqlAlchemyConnector
from sqlalchemy import Column, Float, Integer
from sqlalchemy.dialects.mysql import DATETIME, INTEGER
from sqlalchemy.orm import Session, declarative_base

FIXED_PACKETS = ['ENG_XACT', 'ENG_LED', 'ENG_PFW', 'ENG_CEB']
VARIABLE_PACKETS = ['SCI_XFI']
PACKET_CADENCE = {'ENG_XACT': 500, 'ENG_CEB': 5}

def open_and_split_packet_file(path: str) -> dict[int, io.BytesIO]:
    with open(path, "rb") as mixed_file:
        stream_by_apid = split_by_apid(mixed_file)
    return stream_by_apid

def read_tlm_defs(path):
    tlm = pd.read_excel(path, sheet_name=None)
    for sheet in tlm.keys():
        tlm[sheet] = tlm[sheet].rename(columns={c: c.strip() for c in tlm[sheet].columns})
        if "Start Byte" in tlm[sheet].columns:
            tlm[sheet]["Bit"] = tlm[sheet]["Start Byte"]*8 + tlm[sheet]["Start Bit"]
    apids = tlm["Overview"].dropna().copy()
    apids.index = [int(x.split("x")[1], 16) for x in apids["APID"]]
    apids.columns = ["Name", "APID", "Size_bytes", "Description", "Size_words", "Size_remainder"]
    apids.loc[:, "Size_bytes"] = apids["Size_bytes"].astype(int)
    return apids, tlm

def get_ccsds_data_type(sheet_type, data_size):
    if data_size > 64:
        return 'fill'
    elif sheet_type[0] == "F":
        return 'float'
    elif sheet_type[0] == "I":
        return 'int'
    elif sheet_type[0] == "U":
        return 'uint'
    else:
        return 'fill'

def get_database_data_type(sheet_type):
    if sheet_type[0] == "F":
        return Float
    elif sheet_type[0] == "I":
        return Integer
    elif sheet_type[0] == "U":
        return INTEGER(unsigned=True)

def get_experimental_database():
    """Sets up a session to connect to the MariaDB punchpipe database"""
    credentials = SqlAlchemyConnector.load("experimental-creds", _sync=True)
    engine = credentials.get_engine()
    session = Session(engine)
    return engine, session

Base = declarative_base()

# Class creation function
def create_class(table_name, columns):
    attrs = {'__tablename__': table_name}
    for name, col_type in columns.items():
        attrs[name] = Column(col_type, primary_key=(name == 'id')) # Assuming 'id' is primary key if present
    return type(table_name, (Base,), attrs)


def create_database_from_tlm(tlm):
    REQUIRED_COLUMNS = ['id', 'CCSDS_VERSION_NUMBER', 'CCSDS_PACKET_TYPE', 'CCSDS_SECONDARY_FLAG',
                        'CCSDS_APID', 'CCSDS_SEQUENCE_FLAG', 'CCSDS_SEQUENCE_COUNT', 'CCSDS_PACKET_LENGTH']
    database_classes = {}
    for packet_name in FIXED_PACKETS:
        columns = {name: INTEGER(unsigned=True) for name in REQUIRED_COLUMNS}
        columns['timestamp'] = DATETIME(fsp=6)
        for i, row in tlm[packet_name].iterrows():
            if i > 6:
                columns[row['Mnemonic']] = get_database_data_type(row['Type'])
        database_classes[packet_name] = create_class(packet_name, columns)

    for packet_name in VARIABLE_PACKETS:
        num_fields = len(tlm[packet_name])
        columns = {name: INTEGER(unsigned=True) for name in REQUIRED_COLUMNS}
        columns['timestamp'] = DATETIME(fsp=6)
        for i, row in tlm[packet_name].iterrows():
            if i > 6 and i != num_fields - 1:  # the expanding packet is assumed to be last so we skip it
                columns[row['Mnemonic']] = get_database_data_type(row['Type'])
        database_classes[packet_name] = create_class(packet_name, columns)

    Base.metadata.create_all(engine)
    return database_classes

def create_packet_definitions(tlm, parse_expanding_fields=True):
    defs = {}
    for packet_name in FIXED_PACKETS:
        fields = []
        for i, row in tlm[packet_name].iterrows():
            if i > 6:  # CCSDSPy doesn't need the primary header but it's in the .xls file, so we skip
                fields.append(PacketField(name=row['Mnemonic'],
                                          data_type=get_ccsds_data_type(row['Type'], row['Data Size']),
                                          bit_length=row['Data Size']))
        pkt = ccsdspy.FixedLength(fields)

        pkt.add_converted_field(
            (f'{packet_name}_HDR_SEC', f'{packet_name}_HDR_USEC'),
            'timestamp',
            converters.DatetimeConverter(
                since=datetime(2000, 1, 1),
                units=('seconds', 'microseconds')
            )
        )

        defs[packet_name] = pkt

    for packet_name in VARIABLE_PACKETS:
        fields = []
        num_fields = len(tlm[packet_name])
        for i, row in tlm[packet_name].iterrows():
            if i > 6 and i != num_fields - 1:  # the expanding packet is assumed to be last
                fields.append(PacketField(name=row['Mnemonic'],
                                          data_type=get_ccsds_data_type(row['Type'], row['Data Size']),
                                          bit_length=row['Data Size']))
            elif i == num_fields - 1 and parse_expanding_fields:
                fields.append(PacketArray(name=row['Mnemonic'],
                                          data_type=get_ccsds_data_type(row['Type']),
                                          bit_length=16,
                                          array_shape="expand"))
        pkt = ccsdspy.VariableLength(fields)

        pkt.add_converted_field(
            (f'{packet_name}_HDR_SEC', f'{packet_name}_HDR_USEC'),
            'timestamp',
            converters.DatetimeConverter(
                since=datetime(2000, 1, 1),
                units=('seconds', 'microseconds')
            )
        )

        defs[packet_name] = pkt
    return defs

if __name__ == "__main__":
    engine, session = get_experimental_database()

    path_to_process = "/Users/mhughes/data/real_punch/RAW_CCSDS_DATA/"
    tlm_path = "/Users/mhughes/Desktop/PUNCH_TLM.xls"


    apids, tlm = read_tlm_defs(tlm_path)
    apid_name2num = {row['Name']: int(row['APID'], base=16) for _, row in apids.iterrows()}
    apid_num2name = {num: name for name, num in apid_name2num.items()}

    database_classes = create_database_from_tlm(tlm)
    defs = create_packet_definitions(tlm, parse_expanding_fields=False)

    paths = sorted(glob(path_to_process + "*.tlm"))
    for i, path in enumerate(paths):
        print(i, len(paths), path)

        contents = open_and_split_packet_file(path)
        parsed = {}
        for packet_name in defs:
            apid_num = apid_name2num[packet_name]
            if apid_num in contents:
                try:
                    parsed[packet_name] = defs[packet_name].load(contents[apid_num], include_primary_header=True)
                except (ValueError, RuntimeError):
                    print(f"FAILED PARSING {packet_name} on {path}")

        pkts = []
        for packet_name in parsed:
            try:
                for packet_num in range(0, len(parsed[packet_name]['CCSDS_APID']), PACKET_CADENCE.get(packet_name, 1)):
                    pkts.append(database_classes[packet_name](**{key: parsed[packet_name][key][packet_num]
                                                           for key in parsed[packet_name].keys()}))
                session.bulk_save_objects(pkts)
                session.commit()
            except (ValueError, sqlalchemy.exc.DataError):
                print(f"FAILED ADDING {packet_name} for {path}")
                session.rollback()

        # with engine.connect() as connection:
        #     for packet_name in parsed:
        #         t1 = time.time()
        #         data = [{k: parsed[packet_name][k][i] for k in parsed[packet_name].keys()}
        #                 for i in range(0, len(parsed[packet_name]['CCSDS_APID']), PACKET_CADENCE.get(packet_name, 1))]
        #         t2 = time.time()
        #         # database_classes[packet_name].__table__.insert().execute(data)
        #         connection.execute(database_classes[packet_name].__table__.insert(), data)
        #         t3 = time.time()
        #         print(packet_name, t2 - t1, t3 - t2)
        #     session.commit()
