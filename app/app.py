import os
import pathlib
import numpy as np
import datetime as dt
import dash
import ast
from dash import dcc
from dash import html
import yaml
import pandas as pd
from datetime import timedelta
from dash.exceptions import PreventUpdate
from dash.dependencies import Input, Output, State
from scipy.stats import rayleigh
from db.api_db import MLOPs_DB_Connect
from db.utils import read_yaml_file

config_path = "mlopsDB_config.yaml"
db_config = read_yaml_file(config_path)
mlops_db = MLOPs_DB_Connect(**db_config)

GRAPH_INTERVAL = os.environ.get("GRAPH_INTERVAL", 5000)

app = dash.Dash(
    __name__,
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)
app.title = "Wind Speed Dashboard"

server = app.server

app_color = {"graph_bg": "#082255", "graph_line": "#007ACE"}

app.layout = html.Div(
    [
        # header
        html.Div(
            [
                html.Div(
                    [
                        html.H4("MONITORING PANEL", className="app__header__title"),
                        html.P(
                            "Displays metrics related to all the important deployed machine learning pipelines.",
                            className="app__header__title--grey",
                        ),
                    ],
                    className="app__header__desc",
                ),
                html.Div(
                    [
                        
                        
                        html.A(
                            html.Img(
                                src=app.get_asset_url("cp_logo.ico"),
                                className="app__menu__img",
                            ),
                            href="https://class-proxima.com",
                        ),
                    ],
                    className="app__header__logo",
                ),
            ],
            className="app__header",
        ),
        html.Div(
            [
                # wind speed
                html.Div(
                    [
                        html.Div(
                            [html.H6("RUN DURATION (MOMENTS PIPELINE)", className="graph__title")]
                        ),
                        dcc.Graph(
                            id="wind-speed",
                            figure=dict(
                                layout=dict(
                                    plot_bgcolor=app_color["graph_bg"],
                                    paper_bgcolor=app_color["graph_bg"],
                                )
                            ),
                        ),
                        dcc.Interval(
                            id="wind-speed-update",
                            interval=int(GRAPH_INTERVAL),
                            n_intervals=0,
                        ),
                    ],
                    className="two-thirds column wind__speed__container",
                ),
                html.Div(
                    [
                        # histogram
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.H6(
                                            "FEED COUNT (MOMENTS)",
                                            className="graph__title",
                                        )
                                    ]
                                ),
                                
                                dcc.Graph(
                                    id="wind-histogram",
                                    figure=dict(
                                        layout=dict(
                                            plot_bgcolor=app_color["graph_bg"],
                                            paper_bgcolor=app_color["graph_bg"],
                                        )
                                    ),
                                ),
                            ],
                            className="graph__container first",
                        ),
                        # wind direction
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.H6(
                                            "TOTAL INCOMING VIDEOS (MOMENTS)", className="graph__title"
                                        )
                                    ]
                                ),
                                dcc.Graph(
                                    id="wind-direction",
                                    figure=dict(
                                        layout=dict(
                                            plot_bgcolor=app_color["graph_bg"],
                                            paper_bgcolor=app_color["graph_bg"],
                                        )
                                    ),
                                ),
                            ],
                            className="graph__container second",
                        ),
                    ],
                    className="one-third column histogram__direction",
                ),
            ],
            className="app__content",
        ),
    ],
    className="app__container",
)


def get_current_time():
    """ Helper function to get the current time in seconds. """

    now = dt.datetime.now()
    total_time = (now.hour * 3600) + (now.minute * 60) + (now.second)
    return total_time


@app.callback(
    Output("wind-speed", "figure"), [Input("wind-speed-update", "n_intervals")]
)
def gen_wind_speed(interval):
    """
    Generate the wind speed graph.

    :params interval: update the graph based on an interval
    """

    data_table = mlops_db.get_table('data_table')
    
    moments = data_table.loc[data_table['model_name']=='moments_pipeline']
    #dup = data_table.loc[data_table['model_name']=='moments_pipeline']
    #dup['date_inlet'] = dup['date_inlet'] + timedelta(days=-1)
    #dup['datetime_inference_end'] = dup['datetime_inference_end'] + timedelta(hours=-3)
    #moments = pd.concat([moments, dup], ignore_index=True)

    moments['duration'] = moments['datetime_inference_end'] - moments['datetime_inference_start']
    moments['duration'] = moments['duration'].apply(lambda x: round(x.total_seconds()/3600, 1))
    moments['date_txt'] = moments['date_inlet'].apply(lambda x:str(x))
    print(list(moments["date_txt"]))
    bar_trace = dict(
        type="bar",
        y=moments['duration'],
        line={"color": "#42C4F7"},
        mode="lines",
        x=moments['date_txt']
    )

    layout = dict(
        plot_bgcolor=app_color["graph_bg"],
        paper_bgcolor=app_color["graph_bg"],
        font={"color": "#fff"},
        height=700,
        xaxis={
            "showline": True,
            "zeroline": False,
            "title": "Date",
            "tickvals":moments['date_inlet'],
            "ticktext":[str(f.day) + " " + f.strftime("%b") for f in list(moments['date_inlet'])]#list(moments["date_txt"])
        },
        yaxis={
            "showgrid": True,
            "showline": True,
            "zeroline": False,
            "title":"Duration (Hours)",
            "gridcolor": app_color["graph_line"],
        },
    )

    return dict(data=[bar_trace], layout=layout)


@app.callback(
    Output("wind-direction", "figure"), [Input("wind-speed-update", "n_intervals")]
)
def gen_wind_direction(interval):
    data_table = mlops_db.get_table('data_table')
    
    moments = data_table.loc[data_table['model_name']=='moments_pipeline']
    #dup = data_table.loc[data_table['model_name']=='moments_pipeline']
    #dup['date_inlet'] = dup['date_inlet'] + timedelta(days=-1)
    #dup['datetime_inference_end'] = dup['datetime_inference_end'] + timedelta(hours=-3)
    #moments = pd.concat([moments, dup], ignore_index=True)

    #moments['feed_count'] = moments['metadata'].apply(lambda x:ast.literal_eval(x)['feed_count'])
    moments['date_txt'] = moments['date_inlet'].apply(lambda x:str(x))
    
    print(list(moments["date_txt"]))
    bar_trace = dict(
        type="bar",
        y=moments['no_of_samples'],
        line={"color": "#42C4F7"},
        mode="lines",
        x=moments['date_txt']
    )

    layout = dict(
        plot_bgcolor=app_color["graph_bg"],
        paper_bgcolor=app_color["graph_bg"],
        font={"color": "#fff"},
        height=350,
        xaxis={
            "showline": True,
            "zeroline": False,
            "title": "",
            "tickvals":moments['date_inlet'],
            "ticktext":[str(f.day) + " " + f.strftime("%b") for f in list(moments['date_inlet'])], #list(moments["date_txt"])
            "tickangle":90,
            "tickfont_size":0.5,
            "title":"Date"
        },
        yaxis={
            "showgrid": True,
            "showline": True,
            "zeroline": False,
            "title":"# videos from School",
            "gridcolor": app_color["graph_line"],
        },
    )

    return dict(data=[bar_trace], layout=layout)

'''
@app.callback(
    Output("wind-histogram", "figure"),
    [Input("wind-speed-update", "n_intervals")],
    [
        State("wind-speed", "figure"),
        State("bin-slider", "value"),
        State("bin-auto", "value"),
    ],
)
'''

@app.callback(
    Output("wind-histogram", "figure"), [Input("wind-speed-update", "n_intervals")]
)

def gen_wind_histogram(interval):
    data_table = mlops_db.get_table('data_table')
    
    moments = data_table.loc[data_table['model_name']=='moments_pipeline']
    #dup = data_table.loc[data_table['model_name']=='moments_pipeline']
    #dup['date_inlet'] = dup['date_inlet'] + timedelta(days=-1)
    #dup['datetime_inference_end'] = dup['datetime_inference_end'] + timedelta(hours=-3)
    #moments = pd.concat([moments, dup], ignore_index=True)

    moments['feed_count'] = moments['metadata'].apply(lambda x:ast.literal_eval(x)['feed_count'])
    moments['date_txt'] = moments['date_inlet'].apply(lambda x:str(x))
    
    print(list(moments["date_txt"]))
    bar_trace = dict(
        type="bar",
        y=moments['feed_count'],
        line={"color": "#42C4F7"},
        mode="lines",
        x=moments['date_txt']
    )

    layout = dict(
        plot_bgcolor=app_color["graph_bg"],
        paper_bgcolor=app_color["graph_bg"],
        font={"color": "#fff"},
        height=350,
        #bargap=0.5,
        xaxis={
            "showline": True,
            "zeroline": False,
            "title": "Date",
            "tickangle":90,
            "tickvals":moments['date_inlet'],
            "ticktext":[str(f.day) + " " + f.strftime("%b") for f in list(moments['date_inlet'])]#list(moments["date_txt"])
        },
        yaxis={
            "showgrid": True,
            "showline": True,
            "zeroline": False,
            "title":"# videos pushed to feed",
            "gridcolor": app_color["graph_line"],
        },
    )

    return dict(data=[bar_trace], layout=layout)

'''
@app.callback(
    Output("bin-auto", "value"),
    [Input("bin-slider", "value")],
    [State("wind-speed", "figure")],
)
'''

'''
def deselect_auto(slider_value, wind_speed_figure):
    """ Toggle the auto checkbox. """

    # prevent update if graph has no data
    if "data" not in wind_speed_figure:
        raise PreventUpdate
    if not len(wind_speed_figure["data"]):
        raise PreventUpdate

    if wind_speed_figure is not None and len(wind_speed_figure["data"][0]["y"]) > 5:
        return [""]
    return ["Auto"]

'''

'''
@app.callback(
    Output("bin-size", "children"),
    [Input("bin-auto", "value")],
    [State("bin-slider", "value")],
)
'''

def show_num_bins(autoValue, slider_value):
    """ Display the number of bins. """

    if "Auto" in autoValue:
        return "# of Bins: Auto"
    return "# of Bins: " + str(int(slider_value))


if __name__ == "__main__":
    app.run_server(debug=True)
