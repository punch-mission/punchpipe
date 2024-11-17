from datetime import datetime, timedelta

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import psutil
from dash import Dash, Input, Output, callback, dash_table, dcc, html

from punchpipe.control.util import get_database_session

REFRESH_RATE = 60  # seconds

column_names = ["flow_id", "flow_level", "flow_run_id",
                "flow_run_name", "flow_type", "call_data", "creation_time", "end_time",
                "priority", "start_time", "state"]
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
        with get_database_session() as session:
            reference_time = now - timedelta(hours=24)
            query = f"SELECT datetime, {machine_stat} FROM health WHERE datetime > '{reference_time}';"
            df = pd.read_sql_query(query, session.connection())
        fig = px.line(df, x='datetime', y=machine_stat)

        return fig
    return app
