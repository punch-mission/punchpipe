from prefect_sqlalchemy import SqlAlchemyConnector
from sqlalchemy import create_engine, text

from punchpipe.controlsegment.db import Base

if __name__ == "__main__":
    credentials = SqlAlchemyConnector.load("mariadb-creds")
    engine = credentials.get_engine()
    Base.metadata.drop_all(bind=engine)
