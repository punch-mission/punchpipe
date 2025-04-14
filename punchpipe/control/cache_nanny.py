import sys

from prefect import flow, get_run_logger
from prefect.variables import Variable

from punchpipe.control.util import load_pipeline_configuration

@flow
def cache_nanny(pipeline_config_path: str):
    logger = get_run_logger()

    pipeline_config = load_pipeline_configuration(pipeline_config_path)
    new_state = pipeline_config['cache_layer']['cache_enabled'] and (sys.version_info.minor >= 13)
    old_state = Variable.get("use_shm_cache", "unset")
    logger.info(f"'cache_enabled' is {old_state}")
    if new_state != old_state:
        Variable.set("use_shm_cache", new_state, overwrite=True)
        logger.info(f"Changed 'cache_enabled' from {old_state} to {new_state}")

    # TODO: Cleanup old cache entries