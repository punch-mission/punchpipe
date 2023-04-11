from datetime import timedelta, datetime

import pandas as pd
import datapane as dp
import plotly.express as px
import numpy as np
from sqlalchemy import and_, or_
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from punchpipe.controlsegment.db import MySQLCredentials
from punchpipe.controlsegment.util import get_database_session
from punchpipe.controlsegment.db import Flow


def _create_overall_blocks(start_date, end_date):
    return [f"## Overall: {start_date.strftime('%Y/%m/%d %H:%M')} to {end_date.strftime('%Y/%m/%d %H:%M')} "]


def _process_level(start_date, end_date, level):
    credentials = MySQLCredentials.load("mysql-cred")
    engine = create_engine(f'mysql+pymysql://{credentials.user}:{credentials.password.get_secret_value()}@localhost/punchpipe')
    session = Session(engine)
    query = session.query(Flow).where(or_(and_(Flow.start_time > start_date, Flow.end_time < end_date, Flow.flow_level == level, Flow.state == 'completed'), and_(Flow.flow_level == level, Flow.state == 'running', Flow.start_time > start_date))).statement

    df = pd.read_sql_query(sql=query, con=engine)

    # filtered_df = df[((df['start_time'] > start_date) * (df['end_time'] < end_date) * (df['flow_type'] == f"Level {level}") * (df['state'] == "completed")) | ((df['state'] == "running") * (df['start_time'] > start_date))]
    # filtered_df.set_index("flow_id")
    if len(df):
        completed = df[
            (df['start_time'] > start_date) * (df['end_time'] < end_date) * (df['flow_type'] == f"Level {level}") * (
                        df['state'] == "completed")]
        completed['duration'] = (completed['end_time'] - completed['start_time']).map(timedelta.total_seconds)
        average_duration = np.mean(completed['duration'])
        stddev_duration = np.std(completed['duration'])
        running_flow_count = len(df[(df['state'] == "running") * (df['start_time'] > start_date)])

        plot = px.histogram(completed, x='duration')
        blocks = [f"## Level {level}: {start_date.strftime('%Y/%m/%d %H:%M')} to {end_date.strftime('%Y/%m/%d %H:%M')}",
                  dp.Group(
                      dp.BigNumber(heading="Average Duration [sec]", value=f"{average_duration:.1f}", change=3.01, is_upward_change=True, is_positive_intent=False),
                      dp.BigNumber(heading='Stddev Duration [sec]', value=f"{stddev_duration: .1f}", prev_value=f"{stddev_duration-1: .1f}"),
                      dp.BigNumber(heading="Number of files written", value=512, prev_value=510),
                      dp.BigNumber(heading="Running flow count", value=running_flow_count),
                      dp.BigNumber(heading="Queued flow count", value=0),
                      columns=3
                  ),
                  dp.Plot(plot),
                  dp.DataTable(df)
                  ]
        stats = []
    else:
        blocks = [f"## Level {level}: {start_date.strftime('%Y/%m/%d %H:%M')} to {end_date.strftime('%Y/%m/%d %H:%M')}",
                  "No data"]
        stats = []
    return blocks, stats


def _create_alert_blocks(start_date, end_date):
    return [f"## Alerts: {start_date.strftime('%Y/%m/%d %H:%M')} to {end_date.strftime('%Y/%m/%d %H:%M')}"]


def generate_monitoring_pages(start_date=datetime.now()-timedelta(days=3), end_date=datetime.now()):
    level0_blocks, level0_stats = _process_level(start_date, end_date, 0)
    level1_blocks, level1_stats = _process_level(start_date, end_date, 1)
    level2_blocks, level2_stats = _process_level(start_date, end_date, 2)
    level3_blocks, level3_stats = _process_level(start_date, end_date, 3)

    # embed into a Datapane app
    app = dp.App(
        dp.Page(title="Overall", blocks=_create_overall_blocks(start_date, end_date)),
        dp.Page(title="Alerts", blocks=_create_alert_blocks(start_date, end_date)),
        dp.Page(title="Level 0", blocks=level0_blocks),
        dp.Page(title="Level 1", blocks=level1_blocks),
        dp.Page(title="Level 2", blocks=level2_blocks),
        dp.Page(title="Level 3", blocks=level3_blocks)
    )

    app.save("monitor.html")


if __name__ == "__main__":
    generate_monitoring_pages(start_date=datetime(2022, 12, 1), end_date=datetime.now())