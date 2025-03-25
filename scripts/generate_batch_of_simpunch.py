from datetime import datetime, timedelta

import pandas as pd

from punchpipe.flows.simulate import simpunch_scheduler_flow

if __name__ == "__main__":
    start_date = datetime(2025, 2, 1)
    end_date = datetime(2025, 4, 1)
    pipeline_config_path = "/Users/mhughes/repos/punchpipe/config.yaml"

    for date_obs in pd.date_range(start_date, end_date, freq=timedelta(minutes=4)):
        simpunch_scheduler_flow(pipeline_config_path=pipeline_config_path, reference_time=date_obs)
