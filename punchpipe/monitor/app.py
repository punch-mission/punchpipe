from datetime import datetime, timedelta

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
from dash import Dash, Input, Output, callback, dash_table, dcc, html
from sqlalchemy import select

from punchpipe.control.db import Flow
from punchpipe.control.util import get_database_session

REFRESH_RATE = 60  # seconds

column_names = [ "flow_level", "flow_type", "state", "priority",
                 "creation_time", "start_time", "end_time",
                 "flow_id", "flow_run_id",
                 "flow_run_name", "call_data"]
schedule_columns =[{'name': v.replace("_", " ").capitalize(), 'id': v} for v in column_names]
PAGE_SIZE = 15

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
        html.Div([
            html.Div(children=[dcc.Graph(id='flow-throughput')], style={'padding': 10, 'flex': 1}),

            html.Div(children=[dcc.Graph(id='flow-duration')], style={'padding': 10, 'flex': 1})
        ], style={'display': 'flex', 'flexDirection': 'row'}),
        dash_table.DataTable(id='flows-table',
                             data=pd.DataFrame({name: [] for name in column_names}).to_dict('records'),
                             columns=schedule_columns,
                             page_current=0,
                             page_size=PAGE_SIZE,
                             page_action='custom',

                             filter_action='custom',
                             filter_query='',

                             sort_action='custom',
                             sort_mode='multi',
                             sort_by=[],
                             style_table={'overflowX': 'auto',
                                          'textAlign': 'left'},
                             ),
        dcc.Interval(
            id='interval-component',
            interval=REFRESH_RATE * 1000,  # in milliseconds
            n_intervals=0)
    ])

    operators = [(['ge ', '>='], '__ge__'),
                 (['le ', '<='], '__le__'),
                 (['lt ', '<'], '__lt__'),
                 (['gt ', '>'], '__gt__'),
                 (['ne ', '!='], '__ne__'),
                 (['eq ', '='], '__eq__'),
                 (['contains '], 'contains'),
                 (['datestartswith '], None),
                ]

    def split_filter_part(filter_part):
        for operator_type, py_method in operators:
            for operator in operator_type:
                if operator in filter_part:
                    name_part, value_part = filter_part.split(operator, 1)
                    name = name_part[name_part.find('{') + 1: name_part.rfind('}')]

                    value_part = value_part.strip()
                    v0 = value_part[0]
                    if (v0 == value_part[-1] and v0 in ("'", '"', '`')):
                        value = value_part[1: -1].replace('\\' + v0, v0)
                    else:
                        try:
                            value = float(value_part)
                        except ValueError:
                            value = value_part

                    # word operators need spaces after them in the filter string,
                    # but we don't want these later
                    return name, operator_type[0].strip(), value, py_method

        return [None] * 4

    @callback(
        Output('flows-table', 'data'),
        Input('interval-component', 'n_intervals'),
        Input('flows-table', "page_current"),
        Input('flows-table', "page_size"),
        Input('flows-table', 'sort_by'),
        Input('flows-table', 'filter_query'))
    def update_flows(n, page_current, page_size, sort_by, filter):
        with get_database_session() as session:
            query = select(Flow)
            for filter_part in filter.split(' && '):
                col_name, operator, filter_value, py_method = split_filter_part(filter_part)
                if col_name is not None:
                    query = query.where(getattr(getattr(Flow, col_name), py_method)(filter_value))
            for col in sort_by:
                sort_column = getattr(Flow, col['column_id'])
                if col['direction'] == 'asc':
                    sort_column = sort_column.asc()
                else:
                    sort_column = sort_column.desc()
                query = query.order_by(sort_column)
            query = query.offset(page_current * page_size).limit(page_size)
            dff = pd.read_sql_query(query, session.connection())

        return dff.to_dict('records')


    def create_card_content(level: int, status: str):
        return [
            dbc.CardBody(
                [
                    html.H5(f"Level {level} Status", className="card-title"),
                    html.P(
                        status,
                        className="card-text",
                    ),
                ]
            ),
        ]

    @callback(
        Output('status-cards', 'children'),
        Input('interval-component', 'n_intervals'),
    )
    def update_cards(n):
        now = datetime.now()
        with get_database_session() as session:
            reference_time = now - timedelta(hours=24)
            query = (f"SELECT SUM(num_images_succeeded), SUM(num_images_failed) "
                     f"FROM packet_history WHERE datetime > '{reference_time}';")
            df = pd.read_sql_query(query, session.connection())
        num_l0_success = df['SUM(num_images_succeeded)'].sum()
        num_l0_fails = df['SUM(num_images_failed)'].sum()
        l0_fraction = num_l0_success / (1 + num_l0_success + num_l0_fails)  # add one to avoid div by 0 errors
        if l0_fraction > 0.95 or (num_l0_success + num_l0_fails) == 0:
            l0_status = f"Good ({num_l0_success} : {num_l0_fails})"
            l0_color = "success"
        else:
            l0_status = f"Bad ({num_l0_success} : {num_l0_fails})"
            l0_color = "danger"

        cards = html.Div(
            [
                dbc.Row(
                    [
                        dbc.Col(dbc.Card(create_card_content(0, l0_status), color=l0_color, inverse=True)),
                        dbc.Col(dbc.Card(create_card_content(1, "Good"), color="success", inverse=True)),
                        dbc.Col(dbc.Card(create_card_content(2, "Good"), color="success", inverse=True)),
                        dbc.Col(dbc.Card(create_card_content(3, "Good"), color="success", inverse=True)),
                    ],
                    className="mb-4",
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

    @callback(
        Output('flow-throughput', 'figure'),
        Output('flow-duration', 'figure'),
        Input('interval-component', 'n_intervals'),
    )
    def update_flow_stats(n):
        now = datetime.now()
        with get_database_session() as session:
            reference_time = now - timedelta(hours=72)
            query = ("SELECT flow_type, end_time AS hour, AVG(TIMEDIFF(end_time, start_time)) AS duration, "
                     "COUNT(*) AS count, state "
                     f"FROM flows WHERE end_time > '{reference_time}' "
                     "GROUP BY HOUR(end_time), DAY(end_time), MONTH(end_time), YEAR(end_time), flow_type, state;")
            df = pd.read_sql_query(query, session.connection())
        # Fill missing entries (for hours where nothing ran)
        df.hour = [ts.floor('h') for ts in df.hour]
        dates = pd.date_range(reference_time, now, freq=timedelta(hours=1)).floor('h')
        additions = []
        for flow_type in df.flow_type.unique():
            for date in dates:
                for state in ['failed', 'completed']:
                    if len(df.query('hour == @date and state == @state and flow_type == @flow_type')) == 0:
                        additions.append([flow_type, date, None, 0, state])
        df = pd.concat([df, pd.DataFrame(additions, columns=df.columns)], ignore_index=True)
        df.sort_values(['state', 'hour'], inplace=True)

        # Extrapolate the last hourly window
        now_index = pd.Timestamp(now).floor('h')
        seconds_into_hour = (now - now_index).total_seconds()
        # But don't do it if it's really a lot of extrapolation
        if seconds_into_hour > 120:
            df = df.astype({"count": "float"})
            df.loc[df['hour'] == now_index, 'count'] *= 3600 / (now - now_index).total_seconds()

        fig_throughput = px.line(df, x='hour', y="count", color="flow_type", line_dash="state",
                                 title="Flow throughput (current hour's throughput is extrapolated)")
        fig_throughput.update_xaxes(title_text="Time")
        fig_throughput.update_yaxes(title_text="Flow runs per hour")
        fig_duration = px.line(df[df['state'] == 'completed'], x='hour', y="duration", color="flow_type",
                               title="Flow duration")
        fig_duration.update_xaxes(title_text="Time")
        fig_duration.update_yaxes(title_text="Average flow duration (s)")

        return fig_throughput, fig_duration
    return app
