from sqlalchemy import create_engine

from punchpipe.controlsegment.db import Base
from prefect_sqlalchemy.credentials import DatabaseCredentials


if __name__ == "__main__":
    # Base = declarative_base()
    credentials = DatabaseCredentials.load("mysql-cred")
    engine = create_engine(
        f'mysql+pymysql://{credentials.user}:{credentials.password.get_secret_value()}@localhost/punchpipe')
    Base.metadata.create_all(engine)
