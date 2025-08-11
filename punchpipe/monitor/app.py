import dash_bootstrap_components as dbc
from dash import Dash, dcc, html, page_registry, page_container

from punchpipe.control.util import get_database_session

session = get_database_session()


def create_app():
    app = Dash(external_stylesheets=[dbc.themes.BOOTSTRAP], use_pages=True, pages_folder="monitor/pages")

    app.layout = html.Div([
        html.H1('PUNCHPipe dashboard'),
        html.Div([
            dcc.Link(f"{page['name']}", href=page["relative_path"], style={"margin": "10px"})
            for page in page_registry.values()
        ]),
        page_container
    ])

    return app
