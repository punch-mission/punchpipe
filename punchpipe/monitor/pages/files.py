from datetime import date, datetime, timedelta

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import dash
from dash import Input, Output, callback, dash_table, dcc, html
from sqlalchemy import select, func

from punchpipe.control.db import File
from punchpipe.monitor.app import get_database_session


REFRESH_RATE = 60  # seconds

USABLE_COLUMNS = ["Level", "File type", "Observatory", "File version", "Polarization", "State"]
PAGE_SIZE = 100

dash.register_page(__name__)


layout = html.Div([
            dbc.Row([
                dbc.Col([
                    html.Div([
                        "Show in Table: ",
                        dcc.Checklist(
                            USABLE_COLUMNS,
                            ["File type", "Observatory"],
                            inline=True,
                            id='show-in-table',
                            inputStyle={"margin-left": "10px", "margin-right": "3px"},
                            persistence=True, persistence_type='memory',
                        ),
                    ]),
                    html.Div([
                        "Group by: ",
                        dcc.Checklist(
                            USABLE_COLUMNS,
                            ["File type", "Observatory"],
                            inline=True,
                            id='group-by',
                            inputStyle={"margin-left": "10px", "margin-right": "3px"},
                            persistence=True, persistence_type='memory',
                        ),
                    ]),
                ], width='auto', className='gx-5'),
                dbc.Col([
                    "date_obs:",
                    dcc.DatePickerRange(id='table-date-obs',
                                    min_date_allowed=date(2025, 3, 1),
                                    max_date_allowed=date.today() + timedelta(days=1),
                                    end_date=date.today() + timedelta(days=1),
                                    start_date=date.today() - timedelta(days=31),
                                    initial_visible_month=date.today(),
                                    persistence=True, persistence_type='memory',
                                    ),
                    html.Div([
                        "Extra filters/shortcuts: ",
                        dcc.Checklist(
                            ["Existing files", "Failed files"],
                            ["Existing files"],
                            inline=True,
                            id='extra-filters',
                            inputStyle={"margin-left": "10px", "margin-right": "3px"},
                            persistence=True, persistence_type='memory',
                        ),
                    ]),
                ], width='auto', className='gx-5'),
                dbc.Col([
                    "date_created:",
                    dcc.DatePickerRange(id='table-date-created',
                                    min_date_allowed=date(2025, 3, 1),
                                    max_date_allowed=date.today() + timedelta(days=1),
                                    end_date=date.today() + timedelta(days=1),
                                    start_date=date.today() - timedelta(days=31),
                                    initial_visible_month=date.today(),
                                    persistence=True, persistence_type='memory',
                                    ),
                ], width='auto', className='gx-5'),
        ]),
        dash_table.DataTable(id='files-table',
                             data=pd.DataFrame({name: [] for name in USABLE_COLUMNS + ['Count']}).to_dict('records'),
                             columns=[{'name': col, 'id': col.lower().replace(' ', '_')} for col in USABLE_COLUMNS + ['Count']],
                             page_current=0,
                             page_size=PAGE_SIZE,
                             page_action='custom',

                             filter_action='custom',
                             filter_query='{file_type} = CR,P*',

                             sort_action='custom',
                             sort_mode='multi',
                             sort_by=[dict(column_id='file_type', direction='asc'), dict(column_id='observatory', direction='asc')],
                             style_table={'overflowX': 'auto',
                                          'textAlign': 'left'},
                             fill_width=False,
                             persistence=True, persistence_type='memory',
                             ),
        html.Hr(),
        dbc.Row([
            dbc.Col(width='auto', align='center', children=[
                html.Div([
                    "Color points by: ",
                    dcc.Dropdown(
                        id="graph-color",
                        options=["Nothing", "Level", "Flow type", "File type", "Observatory", "File version", "Polarization", "State"],
                        value="Observatory",
                        clearable=False,
                        style={'width' :'400px'},
                        persistence=True, persistence_type='memory',
                    ),
                ]),
            ]),
            dbc.Col(width='auto', align='center', children=[
                html.Div([
                    "Set shapes by: ",
                    dcc.Dropdown(
                        id="graph-shape",
                        options=["Nothing", "Level", "Flow type", "File type", "Observatory", "File version", "Polarization", "State"],
                        value="Nothing",
                        clearable=False,
                        style={'width' :'400px'},
                        persistence=True, persistence_type='memory',
                    ),
                ]),
            ]),
            dbc.Col(width='auto', align='center', children=[
                html.Div([
                    "X axis: ",
                    dcc.RadioItems(
                        id="graph-x-axis",
                        options=["date_obs", "date_created"],
                        value="date_obs",
                        inputStyle={"margin-left": "10px", "margin-right": "3px"},
                        inline=True,
                        persistence=True, persistence_type='memory',
                    ),
                ]),
            ]),
        ]),
        dcc.Graph(id='file-graph', style={'height': '400'}),
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
                        if operator in ['=', 'eq', 'contains ']:
                            if ',' in value:
                                value = value.split(',')
                                py_method = 'in_'
                                new_value = []
                                for v in value:
                                    if len(v) > 1 and v[1] == '*':
                                        for suffix in ['R', 'M', 'Z', 'P']:
                                            new_value.append(v[0] + suffix)
                                    else:
                                        new_value.append(v)
                                value = new_value
                                value = [v.strip() for v in value]
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


def construct_base_query(columns, filter, extra_filters, include_count, date_obs_start,
                 date_obs_end, date_created_start, date_created_end):
    cols = [getattr(File, col.lower().replace(' ', '_')) for col in columns]
    if include_count:
        cols += [func.count(File.file_id).label("count")]
    query = select(*cols)
    for filter_part in filter.split(' && '):
        col_name, operator, filter_value, py_method = split_filter_part(filter_part)
        if col_name is not None:
            query = query.where(getattr(getattr(File, col_name), py_method)(filter_value))

    if 'Existing files' in extra_filters:
        query = query.where(File.state.in_(['created', 'progressed']))
    if 'Failed files' in extra_filters:
        query = query.where(File.state == 'failed')

    query = query.where(File.date_obs >= date_obs_start).where(File.date_obs <= date_obs_end)
    query = query.where(File.date_created >= date_created_start).where(File.date_created <= date_created_end)
    return query


@callback(
    Output('files-table', 'data'),
    Input('show-in-table', 'value'),
    Input('group-by', 'value'),
    Input('interval-component', 'n_intervals'),
    Input('files-table', "page_current"),
    Input('files-table', "page_size"),
    Input('files-table', 'sort_by'),
    Input('files-table', 'filter_query'),
    Input('extra-filters', 'value'),
    Input('table-date-obs', 'start_date'),
    Input('table-date-obs', 'end_date'),
    Input('table-date-created', 'start_date'),
    Input('table-date-created', 'end_date'),
)
def update_table(show_in_table, group_by, n, page_current, page_size, sort_by, filter, extra_filters, date_obs_start,
                 date_obs_end, date_created_start, date_created_end):
    query = construct_base_query(group_by, filter, extra_filters, True, date_obs_start,
             date_obs_end, date_created_start, date_created_end)
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
    with get_database_session() as session:
        dff = pd.read_sql_query(query, session.connection())

    return dff.to_dict('records')


@callback(
    Output('files-table', 'columns'),
    Output('group-by', 'value'),
    Output('group-by', 'options'),
    Input('show-in-table', 'value'),
    Input('group-by', 'options'),
    Input('group-by', 'value'),
)
def update_table_columns(show_in_table, group_by_options, group_by_selection):
    table_columns = [{'name': col, 'id': col.lower().replace(' ', '_')} for col in USABLE_COLUMNS if col in show_in_table]
    table_columns.append({'name': 'Count', 'id': 'count'})
    group_by_selection = [c for c in group_by_selection if c in show_in_table]
    group_by_options = [{'value': c, 'label': c, 'disabled': c not in show_in_table} for c in USABLE_COLUMNS]
    return table_columns, group_by_selection, group_by_options


def make_keys(dff):
    joinables = []
    columns = list(dff.columns)
    if 'file_type' in columns and 'observatory' in columns:
        joinables.append(dff['file_type'] + dff['observatory'])
        columns.remove('file_type')
        columns.remove('observatory')
    if 'date_obs' in columns:
        columns.remove('date_obs')
    if 'date_created' in columns:
        columns.remove('date_created')
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
    Output('file-graph', 'style'),
    Input('interval-component', 'n_intervals'),
    Input('group-by', 'value'),
    Input('files-table', 'filter_query'),
    Input('files-table', 'sort_by'),
    Input('graph-color', 'value'),
    Input('graph-shape', 'value'),
    Input('extra-filters', 'value'),
    Input('graph-x-axis', 'value'),
    Input('table-date-obs', 'start_date'),
    Input('table-date-obs', 'end_date'),
    Input('table-date-created', 'start_date'),
    Input('table-date-created', 'end_date'),
)
def update_file_graph(n, group_by, filter, sort_by, color_key, shape_key, extra_filters, graph_x_axis, date_obs_start,
                 date_obs_end, date_created_start, date_created_end):
    group_by = [col.lower().replace(' ', '_') for col in group_by]
    color_key = color_key.lower().replace(' ', '_')
    shape_key = shape_key.lower().replace(' ', '_')

    query_cols = group_by + [graph_x_axis]

    exclude_color_from_keys = False
    if color_key not in query_cols and color_key != 'nothing':
        exclude_color_from_keys = True
        query_cols.append(color_key)

    exclude_shape_from_keys = False
    if shape_key not in query_cols and shape_key != 'nothing':
        exclude_shape_from_keys = True
        query_cols.append(shape_key)

    query = construct_base_query(query_cols, filter, extra_filters, False, date_obs_start,
                 date_obs_end, date_created_start, date_created_end)
    with get_database_session() as session:
        dff = pd.read_sql_query(query, session.connection())

    if color_key != 'nothing':
        color_data = dff[color_key]
        if exclude_color_from_keys:
            dff = dff.drop(color_key, axis=1)
    if shape_key != 'nothing':
        shape_data = dff[shape_key]
        if exclude_shape_from_keys:
            dff = dff.drop(shape_key, axis=1)
    keys = make_keys(dff)

    columns = [keys, dff[graph_x_axis]]
    keys = ['name', graph_x_axis]
    if color_key != 'nothing':
        columns.append(color_data)
        keys.append(color_key)
    if shape_key != 'nothing':
        columns.append(shape_data)
        keys.append(shape_key)
    plot_df = pd.concat(columns, axis=1, keys=keys).dropna()

    category_orders = {}
    if sort_by:
        sort_columns = [col['column_id'] for col in sort_by]
        sort_ascending = [col['direction'] == 'asc' for col in sort_by]
        group_cols = group_by
        label_order_df = dff.groupby(group_cols, as_index=False).first().sort_values(sort_columns, ascending=sort_ascending)
        labels = list(make_keys(label_order_df))
        category_orders['name'] = labels
    else:
        labels = plot_df['name'].unique()

    if color_key != 'nothing':
        category_orders[color_key] = sorted(plot_df[color_key].unique())
    if shape_key != 'nothing':
        category_orders[shape_key] = sorted(plot_df[shape_key].unique())

    fig = px.scatter(plot_df, x=graph_x_axis, y='name',
                     color=color_key if color_key != 'nothing' else None,
                     symbol=shape_key if shape_key != 'nothing' else None,
                     category_orders=category_orders,
                     color_discrete_sequence=px.colors.qualitative.D3)
    fig.update_xaxes(title_text=graph_x_axis)

    new_style = {'height': f"{150 + len(labels) * 30}px", 'min-height': '400px'}

    return fig, new_style