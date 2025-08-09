import dash_bootstrap_components as dbc
from dash import Dash, dcc, html, page_registry, page_container


REFRESH_RATE = 60  # seconds

column_names = [ "flow_level", "flow_type", "state", "priority",
                 "creation_time", "launch_time", "start_time", "end_time",
                 "flow_id", "flow_run_id",
                 "flow_run_name", "call_data"]
schedule_columns =[{'name': v.replace("_", " ").capitalize(), 'id': v} for v in column_names]
PAGE_SIZE = 15

def create_app():
    app = Dash(external_stylesheets=[dbc.themes.BOOTSTRAP], use_pages=True, pages_folder="monitor/pages")

    app.layout = html.Div([
        html.H1('PUNCHPipe dashboard'),
        html.Div([
            html.Div(
                dcc.Link(f"{page['name']} - {page['path']}", href=page["relative_path"])
            ) for page in page_registry.values()
        ]),
        page_container
    ])

    return app
