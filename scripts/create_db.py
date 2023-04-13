from punchpipe.controlsegment.db import MySQLCredentials, Base, Flow, File
from punchpipe.flows.level1 import level1_scheduler_flow

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import json
from datetime import datetime

if __name__ == "__main__":
    # Base = declarative_base()
    credentials = MySQLCredentials.load("mysql-cred")
    engine = create_engine(
        f'mysql+pymysql://{credentials.user}:{credentials.password.get_secret_value()}@localhost/punchpipe')
    Base.metadata.create_all(engine)
