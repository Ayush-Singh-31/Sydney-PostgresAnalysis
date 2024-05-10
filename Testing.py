import os
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
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
CatchmentSecondaryPath = os.path.join(currentDir, "Data", "SA2 Digital Boundaries", "SA2_2021_AUST_GDA2020.shp")
SA2 = gpd.read_file(CatchmentSecondaryPath)

SA2 = SA2[SA2['GCC_NAME21'] == 'Greater Sydney']
SA2.plot()
plt.show()
