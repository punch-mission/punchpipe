from datetime import date, datetime, timedelta

import pandas as pd
import plotly.express as px
import dash
from dash import Input, Output, callback, dash_table, dcc, html
from sqlalchemy import select, func

from punchpipe.control.db import File
from punchpipe.control.util import get_database_session


REFRESH_RATE = 60  # seconds

column_names = [ "level", "flow_type", "file_type", "observatory",
                 "file_version", "polarization", "state", "count"]
schedule_columns =[{'name': v.replace("_", " ").capitalize(), 'id': v} for v in column_names]
PAGE_SIZE = 100

dash.register_page(__name__)


layout = html.Div([
        dcc.Checklist(
            ["Level", "Flow type", "File type", "Observatory", "File version", "Polarization", "State"],
            ["File type", "Observatory", "State"],
            inline=True,
            id='group-by',
        ),
        dash_table.DataTable(id='files-table',
                             data=pd.DataFrame({name: [] for name in column_names}).to_dict('records'),
                             columns=schedule_columns,
                             page_current=0,
                             page_size=PAGE_SIZE,
                             page_action='custom',

                             filter_action='custom',
                             filter_query='{file_type} = CR,P* && {state} = created',

                             sort_action='custom',
                             sort_mode='multi',
                             sort_by=[dict(column_id='file_type', direction='asc'), dict(column_id='observatory', direction='asc')],
                             style_table={'overflowX': 'auto',
                                          'textAlign': 'left'},
                             fill_width=False,
                             ),
        html.Div([
            "Color points by: ",
            dcc.Dropdown(
                id="graph-color",
                options=["Level", "Flow type", "File type", "Observatory", "File version", "Polarization", "State"],
                value="Observatory",
                clearable=False,
            ),
        ], style={"width": "50%"}),
        dcc.DatePickerRange(id='plot-range',
                            min_date_allowed=date(2025, 3, 1),
                            max_date_allowed=date.today() + timedelta(days=1),
                            end_date=date.today() + timedelta(days=1),
                            start_date=date.today() - timedelta(days=31),
                            ),
        dcc.Graph(id='file-graph'),
        dcc.Interval(
            id='interval-component',
            interval=REFRESH_RATE * 1000,  # in milliseconds
            n_intervals=0)
    ], style={'margin': '10px'})

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
                        if operator in ['=', 'eq', 'contains']:
                            if ',' in value:
                                value = value.split(',')
                                py_method = 'in_'
                                new_value = []
                                for v in value:
                                    if v[1] == '*':
                                        for suffix in ['R', 'M', 'Z', 'P']:
                                            new_value.append(v[0] + suffix)
                                    else:
                                        new_value.append(v)
                                value = new_value
                            elif value[0] == '*':
                                value = value[1:]
                                py_method = 'endswith'
                            elif value[-1] == '*':
                                value = value[:-1]
                                py_method = 'startswith'


                # word operators need spaces after them in the filter string,
                # but we don't want these later
                return name, operator_type[0].strip(), value, py_method

    return [None] * 4


def construct_base_query(columns, filter, include_count):
    cols = [getattr(File, col.lower().replace(' ', '_')) for col in columns]
    if include_count:
        cols += [func.count(File.file_id).label("count")]
    query = select(*cols)
    for filter_part in filter.split(' && '):
        col_name, operator, filter_value, py_method = split_filter_part(filter_part)
        if col_name is not None:
            query = query.where(getattr(getattr(File, col_name), py_method)(filter_value))
    return query


@callback(
    Output('files-table', 'data'),
    Input('group-by', 'value'),
    Input('interval-component', 'n_intervals'),
    Input('files-table', "page_current"),
    Input('files-table', "page_size"),
    Input('files-table', 'sort_by'),
    Input('files-table', 'filter_query'))
def update_table(group_by, n, page_current, page_size, sort_by, filter):
    with get_database_session() as session:
        query = construct_base_query(group_by, filter, True)
        for col in sort_by:
            sort_column = getattr(File, col['column_id'])
            if col['direction'] == 'asc':
                sort_column = sort_column.asc()
            else:
                sort_column = sort_column.desc()
            query = query.order_by(sort_column)
        for col in group_by:
            query = query.group_by(col.lower().replace(' ', '_'))
        query = query.offset(page_current * page_size).limit(page_size)
        dff = pd.read_sql_query(query, session.connection())

    return dff.to_dict('records')


@callback(
    Output('files-table', 'columns'),
    Input('group-by', 'value'),)
def update_table_columns(group_by):
    cols = [{'label': col, 'id': col.lower().replace(' ', '_')} for col in group_by + ['Count']]
    return cols


def make_keys(dff):
    joinables = []
    columns = list(dff.columns)
    if 'file_type' in columns and 'observatory' in columns:
        joinables.append(dff['file_type'] + dff['observatory'])
        columns.remove('file_type')
        columns.remove('observatory')
    columns.remove('date_obs')
    for column in columns:
        if len(dff[column].unique()) > 1:
            joinables.append(dff[column])
    if len(joinables) == 0:
        joinables = [dff[columns[0]]]
    keys = joinables[0].copy()
    for col in joinables[1:]:
        keys += ' ' + col
    return keys


@callback(
    Output('file-graph', 'figure'),
    Input('interval-component', 'n_intervals'),
    Input('group-by', 'value'),
    Input('files-table', 'filter_query'),
    Input('files-table', 'sort_by'),
    Input('graph-color', 'value'),
    Input('plot-range', 'start_date'),
    Input('plot-range', 'end_date'),
)
def update_file_graph(n, group_by, filter, sort_by, color_key, start_date, end_date):
    group_by = [col.lower().replace(' ', '_') for col in group_by]
    color_key = color_key.lower().replace(' ', '_')
    query_cols = group_by + ['date_obs']
    exclude_color_from_keys = False
    if color_key not in query_cols:
        exclude_color_from_keys = True
        query_cols.append(color_key)
    query = construct_base_query(query_cols, filter, False)
    query = query.where(File.date_obs >= start_date).where(File.date_obs <= end_date)
    with get_database_session() as session:
        dff = pd.read_sql_query(query, session.connection())

    color_data = dff[color_key]
    if exclude_color_from_keys:
        dff = dff.drop(color_key, axis=1)
    keys = make_keys(dff)

    plot_df = pd.concat((keys, dff['date_obs'], color_data), axis=1, keys=['name', 'date_obs', color_key]).dropna()

    if sort_by:
        sort_columns = [col['column_id'] for col in sort_by]
        sort_ascending = [col['direction'] == 'asc' for col in sort_by]
        group_cols = group_by
        label_order_df = dff.groupby(group_cols, as_index=False).first().sort_values(sort_columns, ascending=sort_ascending)
        labels = list(make_keys(label_order_df))
        kwargs = dict(category_orders={"name": labels})
    else:
        kwargs = {}

    fig = px.scatter(plot_df, x='date_obs', y='name', color=color_key, **kwargs)
    fig.update_xaxes(title_text="date_obs")

    return fig