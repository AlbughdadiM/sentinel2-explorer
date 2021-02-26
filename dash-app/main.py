# Import required libraries
import os
import pickle
import copy
import datetime as dt
import math
from datetime import date

import numpy as np
import requests
import pandas as pd
import geopandas as gpd
from flask import Flask
import dash
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
from shared.tools import query_sentinel,make_safe_dirs,query_sentinel_with_polygon
import plotly.express as px
import plotly.graph_objs as go
import json
import dash_leaflet as dl
from dash.exceptions import PreventUpdate
import urllib.parse


mapbox_access_token = "YOUR_MAPBOX_TOKEN"
gdf = gpd.read_file('data/sentinel2_tiles_world/sentinel2_tiles_world.shp')
dummy_pos = [0, 0]
dlatlon2 = 1e-2  

options_tiles = [{'label':'T'+tile_id,'value':tile_id} for tile_id in gdf['Name']]
cloud_marks = {i*5:str(i*5) for i in range(0,20)}

app = dash.Dash(__name__)
server = app.server

app.layout = html.Div(
    [
        dcc.Store(id='polygon_data'),
        dcc.Store(id='drawn_polygon'),
        html.Div(
            [
                html.Div(
                    [
                        html.H1(
                            'Sentinel-2 Explorer',

                        )
                    ],

                    className='eight columns'
                ),
                html.Img(
                    src="https://s3-us-west-1.amazonaws.com/plotly-tutorials/logo/new-branding/dash-logo-by-plotly-stripe.png",
                    className='two columns',
                ),
                html.A(
                    html.Button(
                        "Learn More",
                        id="learnMore"

                    ),
                    href="https://albughdadim.github.io/",
                    className="two columns"
                )
            ],
            id="header",
            className='row',
        ),
        html.Div(
            [
                html.Div(
                    [
                        html.P(
                            'Select Tile of Interest',
                            className = "control_label"
                        ),
                        dcc.Dropdown(
                            id = 'tile_selector',
                            options = options_tiles,
                            value = None,
                            className = 'dcc_control'
                        ),
                        html.P(
                            'Select Search Dates',
                            className="control_label"
                        ),
                        html.Div(
                            dcc.DatePickerRange(
                                id = 'date_picker',
                                min_date_allowed = date(2015, 10, 1),
                                max_date_allowed = date(2022, 1, 1),
                                initial_visible_month = date(2017, 8, 5),
                                start_date = date(2017,8,5),
                                end_date = date(2017, 8, 25)
                                ),
                                className='dcc_control'
                        ),
                        html.P(
                            'Cloud Cover',
                            className = "control_label"
                        ),
                        html.Div(
                        dcc.Slider(
                            id='cloud_selector',
                            min=0,
                            max=100,
                            step=None,
                            marks=cloud_marks,
                            value=5
                            ),
                            className='dcc_control'
                        ),
                        html.Div(
                        dcc.RadioItems(
                            id='processing_level',
                            options=[
                                {'label': 'Level L1C', 'value': 'l1c'},
                                {'label': 'LeveL L2A', 'value': 'l2a'},
                            ],
                            value='l2a'
                            ) ,
                            className='dcc_control'
                        ),
                        html.Div([html.P('')],style={'width': '100%', 'height': '5vh', 'margin': "auto", "display": "block"}),
                        html.A(html.Button('Clear Results'),href='/'),  
                    ],
                    className="pretty_container six columns"
                ),
                html.Div(
                    children = [
                        dl.Map([dl.TileLayer(), 
                        dl.LocateControl(options={'locateOptions': {'enableHighAccuracy': True}}),
                        dl.Polyline(id='POLYLINE_ID', positions=[dummy_pos]),  # Create a polyline, cannot be empty at the moment
                        dl.Polygon(id='POLYGON_ID', positions=[dummy_pos]), 
                        ],
                        id="map", style={'width': '100%', 'height': '40vh', 'margin': "auto", "display": "block"}),
                    ],
                    className='pretty_container six columns',
                ),
            ],
            className="row"
        ),
        html.Div(
            [
                html.Div(
                    [
                        dcc.Graph(id='main_graph')
                    ],
                    className='pretty_container six columns',
                ),
                html.Div(
                    [
                        html.Div(
                            [
                          
                                html.Div(
                                    [
                                        html.Div(
                                            [
                                                html.P("No. of Images In Searched Period"),
                                                html.H6(
                                                    id="s_images_text",
                                                    className="info_text"
                                                )
                                            ],
                                            id="s_images",
                                            className="pretty_container"
                                        ),
                                        html.Div(
                                            [
                                                html.P("Average Cloud Coverage"),
                                                html.H6(
                                                    id="cloud_cover_text",
                                                    className="info_text"
                                                )
                                            ],
                                            id="cloud_cover",
                                            className="pretty_container"
                                        ),
                                        html.Div(
                                            [
                                                html.P("Sum Total Size in GB"),
                                                html.H6(
                                                    id="total_size_text",
                                                    className="info_text"
                                                )
                                            ],
                                            id="total_size",
                                            className="pretty_container"
                                        ),

                                        html.Div(
                                            [
                                                html.P("No. of Valid Geometries"),
                                                html.H6(
                                                    id="v_geom_text",
                                                    className="info_text"
                                                )
                                            ],
                                            id="v_geom",
                                            className="pretty_container"
                                        ),
                                    ],
                                    id="tripleContainer",
                                )

                            ],
                            id="infoContainer",
                            className="row"
                        ),
                        html.Div(
                            [
                            html.P(
                                'Select Image',
                                className = "control_label"
                            ),    
                            dcc.Dropdown(
                                id = 'image_selector',
                                options = [],
                                value = None,
                                className = 'dcc_control'
                            ),
                            html.P(
                                'Export list of images',
                                className="control_label"
                            ),
                            html.Br(),
                            html.A('Download list of products', id = 'list_products'),
                            ],
                            id="countGraphContainer",
                            className="pretty_container"
                        )
                    ],
                    id="rightCol",
                    className="six columns"
                ),
            ],
            className='row'
        ),
        html.Div(
            [
                html.Div(
                    [
                        html.P("Download Links"),
                        html.Div(
                            id="download_links",
                            children = [
                                html.Div([html.P('')],style={'width': '100%', 'height': '50vh', 'margin': "auto", "display": "block"}),
                            ]
                        )
                    ],
                    className='pretty_container six columns',
                ),
                html.Div(
                    [
                        dcc.Graph(id='individual_graph')
                    ],
                    className='pretty_container six columns',
                ),
            ],
            className='row'
        ),
    ],
    id="mainContainer",
    style={
        "display": "flex",
        "flex-direction": "column"
    }
)


@app.callback([Output('POLYLINE_ID', "positions"), Output('POLYGON_ID', "positions"),Output("drawn_polygon","data")],
    Input('map', "click_lat_lng"),
    [State('POLYLINE_ID', "positions"),
    State('POLYGON_ID', "positions")],
    prevent_initial_callbacks=True)
def update_polyline_and_polygon(click_lat_lng, positions, polygon_state):
    if click_lat_lng is None or positions is None:
        raise PreventUpdate()
    # Reset position arrays if polygon array not set to dummy_pos
    if polygon_state[0] != dummy_pos:
        return [dummy_pos], [dummy_pos],None
    # On first click, reset the polyline.
    if len(positions) == 1 and positions[0] == dummy_pos:
        return [click_lat_lng], [dummy_pos],None
    # If the click is close to the first point, close the polygon.
    dist2 = (positions[0][0] - click_lat_lng[0]) ** 2 + (positions[0][1] - click_lat_lng[1]) ** 2
    if dist2 < dlatlon2:
        rev_positions = [tuple(reversed(x)) for x in positions]
        return [dummy_pos], positions,rev_positions
    # Otherwise, append the click position.
    positions.append(click_lat_lng)
    return positions, [dummy_pos],None

@app.callback(Output('download_links','children'),[Input('image_selector','value'),Input('processing_level','value')])
def update_download_links(selected_image,processing_level):
    if selected_image==None:
        raise PreventUpdate()
    else:
        if processing_level == 'l2a':
            processing_level = 'L2A'
        else:
            processing_level = 'L1C'
        download_links = make_safe_dirs(selected_image,processing_level)
        output = []
        for d_link in download_links:
            name = d_link.split('"')[0]
            im_name = os.path.basename(name)
            output.append(html.A(im_name,href=name,className="info_text"))
            output.append(html.Br())
        return output

@app.callback(Output('main_graph','figure'), [Input('tile_selector', 'value'),Input('image_selector', 'value'),Input('polygon_data','data')])
def update_graph_map(selected_tile,selected_image,data_store):
    if selected_tile is None and selected_image is None:
        raise PreventUpdate()
    elif selected_tile and selected_image is None:
        tile_gdf = gdf.loc[gdf['Name'] == selected_tile]
        for _,row in tile_gdf.iterrows():
            x,y = row.geometry.exterior.coords.xy
        return {
                    'data': [
                        dict(
                            type = "scattermapbox",
                            lat = y,
                            lon = x,
                            mode = "lines",
                            fill = 'toself',
                            line = dict(
                            width = 2),
                            text = selected_tile)],
                    "layout": dict(
                            height = 400,
                            autosize = True,
                            margin=dict(t=0, b=0, l=0, r=0),
                            hovermode = "closest",
                            mapbox = dict(
                                accesstoken = mapbox_access_token,
                                bearing = 0,
                                center = dict(lat = np.mean(y), lon = np.mean(x)),
                                pitch = 0,
                                zoom = 5,
                                style='satellite-streets'

                        )
                    )
                }
    elif selected_tile and selected_image:
        tile_gdf = gdf.loc[gdf['Name'] == selected_tile]
        for _,row in tile_gdf.iterrows():
            x,y = row.geometry.exterior.coords.xy
        data_store = json.loads(data_store)
        data_store = gpd.GeoDataFrame.from_features(data_store["features"])
        actual_gdf = data_store.loc[data_store['image'] == selected_image]
        for _,row in actual_gdf.iterrows():
            x1,y1 = row.geometry.exterior.coords.xy
        return {
                    'data': [
                        dict(
                            type = "scattermapbox",
                            lat = y,
                            lon = x,
                            mode = "lines",
                            fill = 'toself',
                            line = dict(
                            width = 2),
                            text = selected_tile),
                        dict(
                            type = "scattermapbox",
                            lat = y1,
                            lon = x1,
                            mode = "lines",
                            fill = 'toself',
                            line = dict(
                            width = 2),
                            text = selected_tile)
                            ],
                    "layout": dict(
                            height = 450,
                            autosize = True,
                            margin=dict(t=0, b=0, l=0, r=0),
                            hovermode = "closest",
                            mapbox = dict(
                                accesstoken = mapbox_access_token,
                                bearing = 0,
                                center = dict(lat = np.mean(y), lon = np.mean(x)),
                                pitch = 0,
                                zoom = 5,
                                style='satellite-streets'

                        )
                    )
                }

    elif selected_image and selected_tile is None:
        data_store = json.loads(data_store)
        data_store = gpd.GeoDataFrame.from_features(data_store["features"])
        actual_gdf = data_store.loc[data_store['image'] == selected_image]
        for _,row in actual_gdf.iterrows():
            x1,y1 = row.geometry.exterior.coords.xy
        return {
                    'data': [
                        dict(
                            type = "scattermapbox",
                            lat = y1,
                            lon = x1,
                            mode = "lines",
                            fill = 'toself',
                            line = dict(
                            width = 2),
                            text = selected_tile)],
                    "layout": dict(
                            height = 400,
                            autosize = True,
                            margin=dict(t=0, b=0, l=0, r=0),
                            hovermode = "closest",
                            mapbox = dict(
                                accesstoken = mapbox_access_token,
                                bearing = 0,
                                center = dict(lat = np.mean(y1), lon = np.mean(x1)),
                                pitch = 0,
                                zoom = 5,
                                style='satellite-streets'

                        )
                    )
                }
        
            



@app.callback([Output('s_images_text','children'),Output('cloud_cover_text','children'),Output('total_size_text','children'),
Output('v_geom_text','children'),Output('individual_graph','figure'),Output('image_selector','options'),Output('polygon_data','data'),
Output('list_products','href')]
,[Input('tile_selector','value'),Input('date_picker','start_date'),Input('date_picker','end_date'),Input('cloud_selector','value'),
Input('drawn_polygon','data'),Input('processing_level','value')])
def update_images(selected_tile,start_date,end_date,cloud_cover,drawn_polygon,processing_level):
    if selected_tile is None and drawn_polygon is None:
        raise PreventUpdate()
    elif selected_tile and drawn_polygon is None:
        avg_cloud_cover,sum_total_size,count_v_geo,polys = query_sentinel(start_date, end_date, selected_tile, cloud=float(cloud_cover),level=processing_level)
        img_options = [{'label':os.path.basename(x),'value':x} for x in polys['image']]
        year_month = [os.path.basename(x).split('_')[2][0:6] for x in polys['image']]
        unique_ym,counts = np.unique(year_month,return_counts=True)
        converted_unique_ym = [dt.datetime.strptime(x,'%Y%m') for x in unique_ym]
        df = pd.DataFrame(dict(ym=converted_unique_ym, counts=counts))
        csv_string = polys.to_csv(index=False, encoding='utf-8',sep = ';')
        csv_string = "data:text/csv;charset=utf-8," + urllib.parse.quote(csv_string)
        trace1 = [go.Bar(
                        x=df['ym'],
                        y=df['counts'],
                        )]
                        
        layout = go.Layout(barmode='group',
                            title='Image Distribution',
                            height = 450,
                            autosize = True,
                            xaxis=dict(
                                title='Year Month',
                                titlefont=dict(
                                    family='verdana',
                                    size=18,
                                    color='#7f7f7f'
                                )
                            ),
                            yaxis=dict(
                                title='# of images',
                                titlefont=dict(
                                    family='verdana',
                                    size=18,
                                    color='#7f7f7f'
                                )
        ))

        return str(len(polys['image'])),str(avg_cloud_cover),str(sum_total_size),str(count_v_geo),{'data':trace1,'layout':layout},\
            img_options,polys.to_json(),csv_string
    elif drawn_polygon and selected_tile is None:
        avg_cloud_cover,sum_total_size,count_v_geo,polys = query_sentinel_with_polygon(drawn_polygon,start_date, end_date,
            cloud=float(cloud_cover),level=processing_level)
        img_options = [{'label':os.path.basename(x),'value':x} for x in polys['image']]
        year_month = [os.path.basename(x).split('_')[2][0:6] for x in polys['image']]
        unique_ym,counts = np.unique(year_month,return_counts=True)
        converted_unique_ym = [dt.datetime.strptime(x,'%Y%m') for x in unique_ym]
        df = pd.DataFrame(dict(ym=converted_unique_ym, counts=counts))
        csv_string = polys.to_csv(index=False, encoding='utf-8',sep = ';')
        csv_string = "data:text/csv;charset=utf-8," + urllib.parse.quote(csv_string)
        trace1 = [go.Bar(
                    x=df['ym'],
                    y=df['counts'],
                    )]
                    
        layout = go.Layout(barmode='group',
                            title='Image Distribution',
                            height = 450,
                            autosize = True,
                            xaxis=dict(
                                title='Year Month',
                                titlefont=dict(
                                    family='verdana',
                                    size=18,
                                    color='#7f7f7f'
                                )
                            ),
                            yaxis=dict(
                                title='# of images',
                                titlefont=dict(
                                    family='verdana',
                                    size=18,
                                    color='#7f7f7f'
                                )
            ))
        return str(len(polys['image'])),str(avg_cloud_cover),str(sum_total_size),str(count_v_geo),{'data':trace1,'layout':layout},\
            img_options,polys.to_json(),csv_string


    else:
        return None,None,None,None,None,None,None,None


app.run_server(port=8080)
