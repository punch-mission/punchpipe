from sqlalchemy import create_engine

from punchpipe.controlsegment.db import Base
from prefect_sqlalchemy.credentials import DatabaseCredentials


if __name__ == "__main__":
    credentials = DatabaseCredentials.load("mariadb-creds")
    engine = credentials.get_engine()
    Base.metadata.create_all(engine)
