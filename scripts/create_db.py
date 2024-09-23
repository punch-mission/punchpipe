from sqlalchemy import create_engine

from punchpipe.controlsegment.db import Base
from prefect_sqlalchemy import SqlAlchemyConnector


if __name__ == "__main__":
    credentials = SqlAlchemyConnector.load("mariadb-creds")
    engine = credentials.get_engine()
    Base.metadata.create_all(engine)
