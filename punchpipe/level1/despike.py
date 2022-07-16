from typing import Optional
from datetime import datetime
from punchpipe.infrastructure.tasks.core import ScienceFunction
from punchpipe.infrastructure.data import PUNCHData
from punchpipe.infrastructure.tasks.core import CalibrationConfiguration
from prefect import task, get_run_logger


@task
def despike(data_object):
    logger = get_run_logger()
    logger.info("despike started")
    # do despiking in here
    logger.info("despike finished")
    data_object.add_history(datetime.now(), "LEVEL1-despike", "image despiked")
    return data_object

