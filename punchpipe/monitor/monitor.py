import os
from datetime import datetime, timedelta

import datapane as dp
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px
from punchbowl.data import PUNCHData
from sqlalchemy import and_, create_engine, or_
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound

from punchpipe.controlsegment.db import (File, FileRelationship, Flow,
                                         MySQLCredentials)


def _create_overall_blocks(start_date, end_date):
    return [f"## Overall: {start_date.strftime('%Y/%m/%d %H:%M')} to {end_date.strftime('%Y/%m/%d %H:%M')} "]


def _process_level(start_date, end_date, level):
    level = int(level)

    credentials = MySQLCredentials.load("mysql-cred")
    engine = create_engine(f'mysql+pymysql://{credentials.user}:{credentials.password.get_secret_value()}@localhost/punchpipe')
    session = Session(engine)
    flow_query = session.query(Flow).where(or_(and_(Flow.start_time > start_date, Flow.end_time < end_date, Flow.flow_level == level), and_(Flow.flow_level == level, Flow.state == 'running', Flow.start_time > start_date))).statement
    file_query = session.query(File).where(and_(File.date_obs > start_date, File.date_obs < end_date, File.level == level)).statement
    query_duration = end_date - start_date
    previous_interval_start = start_date - query_duration
    previous_interval_end = start_date

    previous_flow_query = session.query(Flow).where(
        or_(and_(Flow.start_time > previous_interval_start, Flow.end_time < previous_interval_end, Flow.flow_level == level,
                 Flow.state == 'completed'),
            and_(Flow.flow_level == level, Flow.state == 'running', Flow.start_time > start_date))).statement
    # previous_file_query = session.query(File).where(
    #     and_(File.date_obs > previous_interval_start, File.date_obs < previous_interval_end, File.level == level)).statement

    flow_df = pd.read_sql_query(sql=flow_query, con=engine)
    flow_df['duration'] = (flow_df['end_time'] - flow_df['start_time']).dt.total_seconds()
    flow_df['delay'] = (flow_df['start_time'] - flow_df['creation_time']).dt.total_seconds()
    file_df = pd.read_sql_query(sql=file_query, con=engine)

    previous_flow_df = pd.read_sql_query(sql=previous_flow_query, con=engine)
    previous_flow_df['duration'] = (previous_flow_df['end_time'] - previous_flow_df['start_time']).dt.total_seconds()
    previous_flow_df['delay'] = (previous_flow_df['start_time'] - previous_flow_df['creation_time']).dt.total_seconds()
    # previous_file_df = pd.read_sql_query(sql=previous_file_query, con=engine)

    if len(flow_df):
        completed = flow_df[flow_df['state'] == "completed"]
        previous_completed = previous_flow_df[previous_flow_df['state'] == 'completed']

        failed_flow_count = len(flow_df[flow_df['state'] == 'failed'])

        # completed['duration [sec]'] = (completed['end_time'] - completed['start_time']).map(timedelta.total_seconds)
        # previous_completed['duration [sec]'] = (previous_completed['end_time'] - previous_completed['start_time']).map(timedelta.total_seconds)

        average_duration = np.nanmean(completed['duration'])
        stddev_duration = np.nanstd(completed['duration'])

        previous_average_duration = np.nanmean(previous_completed['duration'])
        previous_stddev_duration = np.nanstd(previous_completed['duration'])

        # planned_count = len(flow_df[flow_df['state'] == 'planned'])
        running_flow_count = len(flow_df[(flow_df['state'] == "running") * (flow_df['start_time'] > start_date)])

        written_count = len(file_df[file_df['state'] == 'created']) + len(file_df[file_df['state'] == 'progressed'])
        failed_file_count = len(file_df[file_df['state'] == 'failed'])

        duration_plot = px.histogram(completed,
                                     x='duration',
                                     labels={'duration': 'duration [sec]'},
                                     title='Histogram of completed flow duration')
        delay_plot = px.histogram(completed,
                                  x='delay',
                                  labels={'delay': 'delay [sec]'},
                                  title='Histogram of completed flow delay to start')
        blocks = [f"## Level {level}: {start_date.strftime('%Y/%m/%d %H:%M')} to {end_date.strftime('%Y/%m/%d %H:%M')}",
                  f"Ran on {datetime.now().strftime('%Y/%m/%d %H:%M')}",
                  dp.Group(
                      dp.BigNumber(heading="Average Duration [sec]",
                                   value=f"{average_duration:.1f}",
                                   prev_value=f"{previous_average_duration:.1f}"),
                      dp.BigNumber(heading='Stddev Duration [sec]',
                                   value=f"{stddev_duration: .1f}",
                                   prev_value=f"{previous_stddev_duration:.1f}"),
                      dp.BigNumber(heading="Number of files written", value=written_count),
                      dp.BigNumber(heading="Number of failed files", value=failed_file_count),
                      dp.BigNumber(heading="Number of failed flows", value=failed_flow_count),
                      dp.BigNumber(heading="Running flow count", value=running_flow_count),
                      columns=3
                  ),
                  dp.Select(blocks=[dp.Plot(duration_plot, label='Duration plot'),
                                    dp.Plot(delay_plot, label='Delay plot')]),
                  dp.DataTable(flow_df, caption='Flow database'),
                  dp.DataTable(file_df, caption='File database')
                  ]
    else:
        blocks = [f"## Level {level}: {start_date.strftime('%Y/%m/%d %H:%M')} to {end_date.strftime('%Y/%m/%d %H:%M')}",
                  "No data"]
    return blocks


def row2table(row):
    """converts a sqlalchemy result row to a string table in markdown """
    table = "| Attribute | Value |\n| --- | --- |\n"
    for column in row.__table__.columns:
        value = str(getattr(row, column.name))
        table += f"| {column.name} | {value} |\n"
    return table


def _file_inquiry(file_id):
    file_id = int(file_id)

    credentials = MySQLCredentials.load("mysql-cred")
    engine = create_engine(
        f'mysql+pymysql://{credentials.user}:{credentials.password.get_secret_value()}@localhost/punchpipe')
    session = Session(engine)

    try:
        file_entry = session.query(File).where(File.file_id == file_id).one()

        # Visualize image
        # TODO: make route path not be hard coded
        fits_path = os.path.join(file_entry.directory("/home/marcus.hughes/running_test"), file_entry.filename())
        data = PUNCHData.from_fits(fits_path)
        fig, ax = plt.subplots(figsize=(4, 4))
        ax.imshow(data.data, origin="lower")

        # Make table
        info_markdown = row2table(file_entry)

        # Provenance
        children_ids = [e.child for e in session.query(FileRelationship).where(FileRelationship.parent == file_id).all()]
        parent_ids = [e.parent for e in session.query(FileRelationship).where(FileRelationship.child == file_id).all()]
        provenance = f"""**Children FileIDs**: {children_ids} \n **Parent FileIDs**: {parent_ids}"""

        return dp.View(f"# FileID={file_id}", dp.Text(info_markdown), dp.Plot(fig), dp.Text(provenance))
    except MultipleResultsFound:
        return dp.View(f"Multiple files with file_id={file_id} found.")
    except NoResultFound:
        return dp.View(f"No file with file_id={file_id} found.")


def _flow_inquiry(flow_id):
    flow_id = int(flow_id)

    credentials = MySQLCredentials.load("mysql-cred")
    engine = create_engine(
        f'mysql+pymysql://{credentials.user}:{credentials.password.get_secret_value()}@localhost/punchpipe')
    session = Session(engine)

    try:
        flow_entry = session.query(Flow).where(Flow.flow_id == flow_id).one()

        # Make table
        info_markdown = row2table(flow_entry)
        return dp.View(f"# FlowID={flow_id}", dp.Text(info_markdown))
    except MultipleResultsFound:
        return dp.View(f"Multiple files with file_id={flow_id} found.")
    except NoResultFound:
        return dp.View(f"No file with file_id={flow_id} found.")


def _create_alert_blocks(start_date, end_date):
    return [f"## Alerts: {start_date.strftime('%Y/%m/%d %H:%M')} to {end_date.strftime('%Y/%m/%d %H:%M')}"]


def level_overview_page():
    return dp.View(
            dp.Form(on_submit=_process_level,
                    controls=dp.Controls(start_date=dp.DateTime(label="Start datetime", initial=datetime.now()-timedelta(days=1)),
                                         end_date=dp.DateTime(label="End datetime", initial=datetime.now()),
                                         level=dp.Choice(options=["0", "1", "2", "3"])
                                         ),
                    )
    )


def file_inquiry_page():
    return dp.View(dp.Form(on_submit=_file_inquiry,
                           controls=dp.Controls(file_id=dp.NumberBox(label="File ID", initial=0))))


def flow_inquiry_page():
    return dp.View(dp.Form(on_submit=_flow_inquiry,
                           controls=dp.Controls(flow_id=dp.NumberBox(label="Flow ID", initial=0))))


def serve_monitoring_pages():
    start_date = datetime.now() - timedelta(days=1)
    end_date = datetime.now()

    # embed into a Datapane app
    app = dp.App(
        dp.Page(title="Overall", blocks=_create_overall_blocks(start_date, end_date)),
        dp.Page(title="Alerts", blocks=_create_alert_blocks(start_date, end_date)),
        dp.Page(title="Level Overview", blocks=level_overview_page()),
        dp.Page(title="File Inquiry", blocks=file_inquiry_page()),
        dp.Page(title="Flow Inquiry", blocks=flow_inquiry_page())
        )
    dp.serve_app(app)


if __name__ == "__main__":
    #generate_monitoring_pages(start_date=datetime(2022, 12, 1), end_date=datetime.now())
    serve_monitoring_pages()
