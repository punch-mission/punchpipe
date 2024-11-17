from datetime import datetime
import psutil

from prefect import flow

from punchpipe.control.util import get_database_session, load_pipeline_configuration
from punchpipe.control.db import Health


@flow
def update_machine_health_stats():
    config = load_pipeline_configuration()

    now = datetime.now()
    cpu_usage = psutil.cpu_percent(interval=5)
    memory_usage = psutil.virtual_memory().used
    memory_percentage = psutil.virtual_memory().percent
    disk_usage = psutil.disk_usage(config.get("root", "/")).used
    disk_percentage = psutil.disk_usage(config.get("root", "/")).percent
    num_pids = len(psutil.pids())

    with get_database_session() as session:
        new_health_entry = Health(datetime=now,
                                  cpu_usage=cpu_usage,
                                  memory_usage=memory_usage,
                                  memory_percentage=memory_percentage,
                                  disk_usage=disk_usage,
                                  disk_percentage=disk_percentage,
                                  num_pids=num_pids)
        session.add(new_health_entry)
        session.commit()
