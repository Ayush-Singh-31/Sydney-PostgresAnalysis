import os
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, String
from shapely.wkt import loads
from shapely.geometry import MultiPolygon
from geoalchemy2 import Geometry, WKTElement

def create_wkt_element(geom, srid):
    if geom.geom_type == 'Polygon':
        geom = MultiPolygon([geom])
    return WKTElement(geom.wkt, srid)

currentDir = os.path.dirname(os.path.abspath(__file__))
CatchmentSecondaryPath = os.path.join(currentDir, "Data", "catchments", "catchments_secondary.shp")
CS = gpd.read_file(CatchmentSecondaryPath)

CS['Geom'] = CS['geometry'].apply(lambda x: create_wkt_element(geom=x, srid=4326))
CS = CS.drop(columns="geometry")
print(CS)