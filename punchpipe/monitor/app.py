from datetime import datetime, timedelta

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import psutil
from dash import Dash, Input, Output, callback, dash_table, dcc, html

from punchpipe.controlsegment.db import Health
from punchpipe.controlsegment.util import get_database_session

REFRESH_RATE = 60  # seconds

column_names = ["call_data", "creation_time", "end_time", "flow_id", "flow_level", "flow_run_id",
                "flow_run_name", "flow_type", "priority", "start_time", "state"]
schedule_columns =[{'name': v, 'id': v} for v in column_names]


def create_app():
    app = Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])
    app.layout = html.Div([
        dcc.Graph(id='machine-graph'),
        dcc.Dropdown(
            id="machine-stat",
            options=["cpu_usage", "memory_usage", "memory_percentage", "disk_usage", "disk_percentage", "num_pids"],
            value="cpu_usage",
            clearable=False,
        ),
        dash_table.DataTable(id='flows',
                             data=pd.DataFrame({name: [] for name in column_names}).to_dict('records'),
                             columns=schedule_columns),
        dcc.Interval(
            id='interval-component',
            interval=REFRESH_RATE * 1000,  # in milliseconds
            n_intervals=0)
    ])

    @callback(
        Output('files', 'data'),
        Input('interval-component', 'n_intervals'),
    )
    def update_flows(n):
        query = "SELECT * FROM flows;"
        with get_database_session() as session:
            df = pd.read_sql_query(query, session.connection())
        return df.to_dict('records')

    @callback(
        Output('machine-graph', 'figure'),
        Input('interval-component', 'n_intervals'),
        Input('machine-stat', 'value'),
    )
    def update_machine_stats(n, machine_stat):
        now = datetime.now()
        cpu_usage = psutil.cpu_percent(interval=None)
        memory_usage = psutil.virtual_memory().used
        memory_percentage = psutil.virtual_memory().percent
        disk_usage = psutil.disk_usage('/').used
        disk_percentage = psutil.disk_usage('/').percent
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

            reference_time = now - timedelta(hours=24)
            query = f"SELECT datetime, {machine_stat} FROM health WHERE datetime > '{reference_time}';"
            df = pd.read_sql_query(query, session.connection())
        fig = px.line(df, x='datetime', y=machine_stat)

        return fig
    return app
