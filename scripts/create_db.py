from punchpipe.controlsegment.db import Flow, FlowKind, File, FileRelationship, MySQLCredentials, Base

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base

if __name__ == "__main__":
    # Base = declarative_base()
    credentials = MySQLCredentials.load("mysql-cred")
    engine = create_engine(
        f'mysql+pymysql://{credentials.user}:{credentials.password.get_secret_value()}@localhost/punchpipe')
    Base.metadata.create_all(engine)
