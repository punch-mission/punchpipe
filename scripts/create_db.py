from sqlalchemy import create_engine, text

from punchpipe.controlsegment.db import Base
from prefect_sqlalchemy import SqlAlchemyConnector


if __name__ == "__main__":
    credentials = SqlAlchemyConnector.load("mariadb-creds")
    engine = credentials.get_engine()
    with engine.connect() as connection:
        result = connection.execute(text('CREATE DATABASE IF NOT EXISTS punchpipe;'))
    Base.metadata.create_all(engine)
