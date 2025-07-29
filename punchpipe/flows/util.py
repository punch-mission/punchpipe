import os
from datetime import datetime

from punchpipe.control.db import File


def file_name_to_full_path(file_name: str | list[str], root_dir: str) -> str | list[str]:
    if isinstance(file_name, list):
        return [file_name_to_full_path(f, root_dir) for f in file_name]
    level = file_name.split("_")[1][1]
    code = file_name.split("_")[2][:2]
    obs = file_name.split("_")[2][-1]
    version = file_name.split("_")[-1].split(".")[0][1:]
    date = datetime.strptime(file_name.split("_")[3], "%Y%m%d%H%M%S")

    file = File(level=level, file_type=code, observatory=obs, file_version=version, software_version="", date_obs=date,
                state='')

    return os.path.join(file.directory(root_dir), file_name)