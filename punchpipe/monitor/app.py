from datetime import datetime, timedelta

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
from dash import Dash, Input, Output, callback, dash_table, dcc, html

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

    operators = [['ge ', '>='],
                 ['le ', '<='],
                 ['lt ', '<'],
                 ['gt ', '>'],
                 ['ne ', '!='],
                 ['eq ', '='],
                 ['contains '],
                 ['datestartswith ']]

    def split_filter_part(filter_part):
        for operator_type in operators:
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
                    return name, operator_type[0].strip(), value

        return [None] * 3

    @callback(
        Output('flows-table', 'data'),
        Input('interval-component', 'n_intervals'),
        Input('flows-table', "page_current"),
        Input('flows-table', "page_size"),
        Input('flows-table', 'sort_by'),
        Input('flows-table', 'filter_query'))
    def update_flows(n, page_current, page_size, sort_by, filter):
        query = "SELECT * FROM flows;"
        with get_database_session() as session:
            dff = pd.read_sql_query(query, session.connection())

        filtering_expressions = filter.split(' && ')
        for filter_part in filtering_expressions:
            col_name, operator, filter_value = split_filter_part(filter_part)

            if operator in ('eq', 'ne', 'lt', 'le', 'gt', 'ge'):
                # these operators match pandas series operator method names
                dff = dff.loc[getattr(dff[col_name], operator)(filter_value)]
            elif operator == 'contains':
                dff = dff.loc[dff[col_name].str.contains(filter_value)]
            elif operator == 'datestartswith':
                # this is a simplification of the front-end filtering logic,
                # only works with complete fields in standard format
                dff = dff.loc[dff[col_name].str.startswith(filter_value)]

        if len(sort_by):
            dff = dff.sort_values(
                [col['column_id'] for col in sort_by],
                ascending=[
                    col['direction'] == 'asc'
                    for col in sort_by
                ],
                inplace=False
            )

        page = page_current
        size = page_size
        return dff.iloc[page * size: (page + 1) * size].to_dict('records')


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
    return app
