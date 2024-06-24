import json
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
from dash import Dash, Input, Output, dash_table, dcc, html
from dash.exceptions import PreventUpdate

app = Dash(__name__)

df = pd.read_csv(
    "/Users/jhughes/Desktop/repos/punchpipe/punchpipe/monitor/sample.csv",
    parse_dates=["creation_time", "start_time", "end_time"],
)
df.set_index("flow_id")
df["duration"] = (df["end_time"] - df["start_time"]).map(timedelta.total_seconds)

fig = px.histogram(df, x="duration")

app.layout = html.Div(
    [
        dcc.DatePickerRange(
            id="date_picker_range",
            min_date_allowed=date(2022, 1, 1),
            max_date_allowed=date.today(),
            initial_visible_month=date(2022, 1, 1),
            start_date=date.today() - timedelta(days=1),
            end_date=date.today(),
        ),
        dcc.Graph(id="duration", figure=fig),
        dash_table.DataTable(
            id="flow_table",
            data=df.to_dict("records"),
            columns=[{"name": i, "id": i} for i in df.columns],
            page_action="none",
            style_table={"height": "300px", "overflowY": "auto"},
            sort_action="native",
        ),
        html.Pre(id="relayout-data"),
    ]
)


@app.callback(
    Output("duration", "figure"), Input("date_picker_range", "start_date"), Input("date_picker_range", "end_date")
)
def update_histogram(start_date, end_date):
    filtered_df = df[(df["start_time"] > start_date) * (df["end_time"] < end_date)]
    fig = px.histogram(filtered_df, x="duration")
    fig.update_layout(transition_duration=500)

    return fig


@app.callback(
    Output("flow_table", "data"), Input("date_picker_range", "start_date"), Input("date_picker_range", "end_date")
)
def update_table(start_date, end_date):
    return df[(df["start_time"] > start_date) * (df["end_time"] < end_date)].to_dict("records")


@app.callback(Output("relayout-data", "children"), Input("duration", "relayoutData"))
def display_relayout_data(relayoutData):
    if relayoutData is None:
        raise PreventUpdate
    elif "xaxis.range[0]" not in relayoutData.keys():
        raise PreventUpdate
    else:
        # get the relevant axis ranges, you can use to drop columns from the datatable
        print(relayoutData, type(relayoutData))
        return json.dumps(relayoutData, indent=2)


if __name__ == "__main__":
    app.run_server(debug=False)
