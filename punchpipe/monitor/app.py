from datetime import datetime, timedelta

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
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
        html.Div(
            id="status-cards"
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
        Output('flows', 'data'),
        Input('interval-component', 'n_intervals'),
    )
    def update_flows(n):
        query = "SELECT * FROM flows;"
        with get_database_session() as session:
            df = pd.read_sql_query(query, session.connection())
        return df.to_dict('records')

    @callback(
        Output('status-cards', 'children'),
        Input('interval-component', 'n_intervals'),
    )
    def update_cards(n):
        card_content = [
            dbc.CardHeader("Card header"),
            dbc.CardBody(
                [
                    html.H5("Card title", className="card-title"),
                    html.P(
                        "This is some card content that we'll reuse",
                        className="card-text",
                    ),
                ]
            ),
        ]

        cards = html.Div(
            [
                dbc.Row(
                    [
                        dbc.Col(dbc.Card(card_content, color="primary", inverse=True)),
                        dbc.Col(
                            dbc.Card(card_content, color="secondary", inverse=True)
                        ),
                        dbc.Col(dbc.Card(card_content, color="info", inverse=True)),
                    ],
                    className="mb-4",
                ),
                dbc.Row(
                    [
                        dbc.Col(dbc.Card(card_content, color="success", inverse=True)),
                        dbc.Col(dbc.Card(card_content, color="warning", inverse=True)),
                        dbc.Col(dbc.Card(card_content, color="danger", inverse=True)),
                    ],
                    className="mb-4",
                ),
                dbc.Row(
                    [
                        dbc.Col(dbc.Card(card_content, color="light")),
                        dbc.Col(dbc.Card(card_content, color="dark", inverse=True)),
                    ]
                ),
            ]
        )
        return cards

    @callback(
        Output('machine-graph', 'figure'),
        Input('interval-component', 'n_intervals'),
        Input('machine-stat', 'value'),
    )
    def update_machine_stats(n, machine_stat):
        axis_labels = {"cpu_usage": "CPU Usage %",
                       "memory_usage": "Memory Usage[GB]",
                       "memory_percentage": "Memory Usage %",
                       "disk_usage": "Disk Usage[GB]",
                       "disk_percentage": "Disk Usage %",
                       "num_pids": "Process Count"}
        now = datetime.now()
        with get_database_session() as session:
            reference_time = now - timedelta(hours=24)
            query = f"SELECT datetime, {machine_stat} FROM health WHERE datetime > '{reference_time}';"
            df = pd.read_sql_query(query, session.connection())
        fig = px.line(df, x='datetime', y=machine_stat, title="Machine stats")
        fig.update_xaxes(title_text="Time")
        fig.update_yaxes(title_text=axis_labels[machine_stat])

        return fig
    return app
