from datetime import timedelta, datetime
from functools import partial

import pandas as pd
import datapane as dp
import plotly.express as px
import numpy as np
from sqlalchemy import and_, or_
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from punchpipe.controlsegment.db import MySQLCredentials
from punchpipe.controlsegment.util import get_database_session
from punchpipe.controlsegment.db import Flow, File


def _create_overall_blocks(start_date, end_date):
    return [f"## Overall: {start_date.strftime('%Y/%m/%d %H:%M')} to {end_date.strftime('%Y/%m/%d %H:%M')} "]


def _process_level(start_date, end_date, level):
    credentials = MySQLCredentials.load("mysql-cred")
    engine = create_engine(f'mysql+pymysql://{credentials.user}:{credentials.password.get_secret_value()}@localhost/punchpipe')
    session = Session(engine)
    flow_query = session.query(Flow).where(or_(and_(Flow.start_time > start_date, Flow.end_time < end_date, Flow.flow_level == level, Flow.state == 'completed'), and_(Flow.flow_level == level, Flow.state == 'running', Flow.start_time > start_date))).statement
    file_query = session.query(File).where(and_(File.date_obs > start_date, File.date_obs < end_date, File.level == level)).statement
    query_duration = end_date - start_date
    previous_interval_start = start_date - query_duration
    previous_interval_end = start_date

    previous_flow_query = session.query(Flow).where(
        or_(and_(Flow.start_time > previous_interval_start, Flow.end_time < previous_interval_end, Flow.flow_level == level,
                 Flow.state == 'completed'),
            and_(Flow.flow_level == level, Flow.state == 'running', Flow.start_time > start_date))).statement
    previous_file_query = session.query(File).where(
        and_(File.date_obs > previous_interval_start, File.date_obs < previous_interval_end, File.level == level)).statement

    flow_df = pd.read_sql_query(sql=flow_query, con=engine)
    file_df = pd.read_sql_query(sql=file_query, con=engine)

    previous_flow_df = pd.read_sql_query(sql=previous_flow_query, con=engine)
    previous_file_df = pd.read_sql_query(sql=previous_file_query, con=engine)

    if len(flow_df):
        completed = flow_df[flow_df['state'] == "completed"]
        previous_completed = previous_flow_df[previous_flow_df['state'] == 'completed']

        completed['duration [sec]'] = (completed['end_time'] - completed['start_time']).map(timedelta.total_seconds)
        previous_completed['duration [sec]'] = (previous_completed['end_time'] - previous_completed['start_time']).map(timedelta.total_seconds)

        average_duration = np.nanmean(completed['duration [sec]'])
        stddev_duration = np.nanstd(completed['duration [sec]'])

        previous_average_duration = np.nanmean(previous_completed['duration [sec]'])
        previous_stddev_duration = np.nanstd(previous_completed['duration [sec]'])

        planned_count = len(flow_df[flow_df['state'] == 'planned'])
        running_flow_count = len(flow_df[(flow_df['state'] == "running") * (flow_df['start_time'] > start_date)])

        written_count = len(file_df[file_df['state'] == 'created']) + len(file_df[file_df['state'] == 'progressed'])
        failed_file_count = len(file_df[file_df['state'] == 'failed'])

        plot = px.histogram(completed, x='duration [sec]')
        blocks = [f"## Level {level}: {start_date.strftime('%Y/%m/%d %H:%M')} to {end_date.strftime('%Y/%m/%d %H:%M')}",
                  dp.Group(
                      dp.BigNumber(heading="Average Duration [sec]",
                                   value=f"{average_duration:.1f}",
                                   prev_value=f"{previous_average_duration:.1f}"),
                      dp.BigNumber(heading='Stddev Duration [sec]',
                                   value=f"{stddev_duration: .1f}",
                                   prev_value=f"{previous_stddev_duration:.1f}"),
                      dp.BigNumber(heading="Number of files written", value=written_count),
                      dp.BigNumber(heading="Number of failed files", value=failed_file_count),
                      dp.BigNumber(heading="Running flow count", value=running_flow_count),
                      dp.BigNumber(heading="Planned flow count", value=planned_count),
                      columns=3
                  ),
                  dp.Plot(plot),
                  dp.DataTable(flow_df, caption='Flow database'),
                  dp.DataTable(file_df, caption='File database')
                  ]
        stats = []
    else:
        blocks = [f"## Level {level}: {start_date.strftime('%Y/%m/%d %H:%M')} to {end_date.strftime('%Y/%m/%d %H:%M')}",
                  "No data"]
        stats = []
    return blocks


def _create_alert_blocks(start_date, end_date):
    return [f"## Alerts: {start_date.strftime('%Y/%m/%d %H:%M')} to {end_date.strftime('%Y/%m/%d %H:%M')}"]


def level_overview_page():
    return dp.View(
            dp.Text("Welcome to my app"),
            dp.Form(on_submit=_process_level,
                    controls=dp.Controls(start_date=dp.DateTime(label="Start", initial=datetime.now()-timedelta(days=1)),
                                         end_date=dp.DateTime(label="End", initial=datetime.now()),
                                         level=dp.Choice(options=[0, 1, 2, 3])),
                    label="Hmm:"),
        )


def serve_monitoring_pages():
    start_date = datetime.now() - timedelta(days=1)
    end_date = datetime.now()

    level0_blocks = _process_level(start_date, end_date, 0)
    level1_blocks = _process_level(start_date, end_date, 1)
    level2_blocks = _process_level(start_date, end_date, 2)
    level3_blocks = _process_level(start_date, end_date, 3)

    # embed into a Datapane app
    app = dp.App(
        dp.Page(title="Overall", blocks=_create_overall_blocks(start_date, end_date)),
        dp.Page(title="Alerts", blocks=_create_alert_blocks(start_date, end_date)),
        dp.Page(title="Level 0", blocks=level0_blocks),
        dp.Page(title="Level 1", blocks=level1_blocks),
        dp.Page(title="Level 3", blocks=level3_blocks),
        dp.Page(title="Level Overview", blocks=level_overview_page()),
        )
    dp.serve_app(app)


if __name__ == "__main__":
    #generate_monitoring_pages(start_date=datetime(2022, 12, 1), end_date=datetime.now())
    serve_monitoring_pages()
