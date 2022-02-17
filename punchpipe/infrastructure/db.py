from sqlalchemy import Column, String
from sqlalchemy.ext.hybrid import hybrid_property
from datetime import datetime
from controlsegment.db import FilesTemplate


class File(FilesTemplate):
    __tablename__ = "files"
    file_type = Column(String(2), nullable=False)
    observatory = Column(String(2), nullable=False)
    polarization = Column(String(2))

    @hybrid_property
    def file_name(self):
        return f"L{self.level}_{self.file_type}{self.observatory}_" \
               f"{datetime.strftime(self.date_acquired, '%Y%m%d%H%M%S')}_v{self.file_version}.fits"
