from typing import Optional
from datetime import datetime
from punchpipe.infrastructure.tasks.core import ScienceFunction
from punchpipe.infrastructure.data import PUNCHData
from punchpipe.infrastructure.tasks.core import CalibrationConfiguration
from prefect import task, get_run_logger


@task
def remove_deficient_pixels(data_object):
    logger = get_run_logger()
    logger.info("remove_deficient_pixels started")
    # do deficient pixel removal in here
    logger.info("remove_deficient_pixels finished")
    data_object.add_history(datetime.now(), "LEVEL1-remove_deficient_pixels", "deficient pixels removed")
    return data_object