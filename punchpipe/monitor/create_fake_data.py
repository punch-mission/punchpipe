import random
import uuid
from datetime import datetime, timedelta
from random import randrange

import coolname
import numpy as np
import pandas as pd


def random_date(start, end):
    """
    This function will return a random datetime between two datetime
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)


def generate_completed_level0():
    flow_id = str(uuid.uuid4())
    flow_type = "Level 0"
    flow_run = "-".join(coolname.generate(2))
    state = "completed"
    creation_time = random_date(datetime(2022, 1, 1), datetime.now())
    start_time = creation_time + timedelta(seconds=random.randint(0, 120))
    end_time = start_time + timedelta(seconds=int(np.random.normal(30, 3)))
    priority = 1
    call_data = ""

    return {
        "flow_id": flow_id,
        "flow_type": flow_type,
        "flow_run": flow_run,
        "state": state,
        "creation_time": creation_time,
        "start_time": start_time,
        "end_time": end_time,
        "priority": priority,
        "call_data": call_data,
    }


def generate_running_level0():
    flow_id = str(uuid.uuid4())
    flow_type = "Level 0"
    flow_run = "-".join(coolname.generate(2))
    state = "running"
    creation_time = random_date(datetime(2022, 1, 1), datetime.now())
    start_time = creation_time + timedelta(seconds=random.randint(0, 120))
    end_time = None
    priority = 1
    call_data = ""

    return {
        "flow_id": flow_id,
        "flow_type": flow_type,
        "flow_run": flow_run,
        "state": state,
        "creation_time": creation_time,
        "start_time": start_time,
        "end_time": end_time,
        "priority": priority,
        "call_data": call_data,
    }


if __name__ == "__main__":
    completed_level0 = [generate_completed_level0() for _ in range(5_000)]
    running_level0 = [generate_running_level0() for _ in range(100)]
    rows = completed_level0 + running_level0
    df = pd.DataFrame(rows)
    print(df)
    df.to_csv("/Users/jhughes/Desktop/repos/punchpipe/punchpipe/monitor/sample.csv")
