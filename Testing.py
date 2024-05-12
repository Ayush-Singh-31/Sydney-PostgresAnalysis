import os, json, pgquery
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, String
from shapely.wkt import loads
from shapely.geometry import MultiPolygon
from geoalchemy2 import Geometry, WKTElement

srid = 4326

def create_wkt_element(geom, srid):
    if geom.geom_type == 'Polygon':
        geom = MultiPolygon([geom])
    return WKTElement(geom.wkt, srid)

def pgconnect(credential_filepath, db_schema="public"):
    with open(credential_filepath) as f:
        db_conn_dict = json.load(f)
        host       = db_conn_dict['host']
        db_user    = db_conn_dict['user']
        db_pw      = db_conn_dict['password']
        default_db = db_conn_dict['user']
        port       = db_conn_dict['port']
        try:
            db = create_engine(f'postgresql+psycopg2://{db_user}:{db_pw}@{host}:{port}/{default_db}', echo=False)
            conn = db.connect()
            print('Connected successfully.')
        except Exception as e:
            print("Unable to connect to the database.")
            print(e)
            db, conn = None, None
        return db,conn

def query(conn, sqlcmd, args=None, df=True):
    result = pd.DataFrame() if df else None
    try:
        if df:
            result = pd.read_sql_query(sqlcmd, conn, params=args)
        else:
            result = conn.execute(text(sqlcmd), args).fetchall()
            result = result[0] if len(result) == 1 else result
    except Exception as e:
        print("Error encountered: ", e, sep='\n')
    return result

SA2DigitalBoundaries = gpd.read_file("/Users/ayush/Documents/University/Year 02/Sem 01/DATA2001/DATA2001Assignment/Data/SA2 Digital Boundaries/SA2_2021_AUST_GDA2020.shp")
SA2DigitalBoundaries.drop(columns=['LOCI_URI21'], inplace=True) 
SA2DigitalBoundaries = SA2DigitalBoundaries[SA2DigitalBoundaries['GCC_NAME21'] == 'Greater Sydney'].copy()

# Renaming the columns
SA2DigitalBoundaries.rename(columns = {'SA2_CODE21':'SA2code'}, inplace = True)
SA2DigitalBoundaries.rename(columns = {'SA2_NAME21':'SA2name'}, inplace = True)
SA2DigitalBoundaries.rename(columns = {'CHG_FLAG21':'CHGFlage'}, inplace = True)
SA2DigitalBoundaries.rename(columns = {'CHG_LBL21':'CHGLabel'}, inplace = True)
SA2DigitalBoundaries.rename(columns = {'SA3_CODE21':'SA3code'}, inplace = True)
SA2DigitalBoundaries.rename(columns = {'SA3_NAME21':'SA3name'}, inplace = True)
SA2DigitalBoundaries.rename(columns = {'SA4_CODE21':'SA4code'}, inplace = True)
SA2DigitalBoundaries.rename(columns = {'SA4_NAME21':'SA4name'}, inplace = True)
SA2DigitalBoundaries.rename(columns = {'GCC_CODE21':'GCCCode'}, inplace = True)
SA2DigitalBoundaries.rename(columns = {'GCC_NAME21':'GCCName'}, inplace = True)
SA2DigitalBoundaries.rename(columns = {'STE_CODE21':'STECode'}, inplace = True)
SA2DigitalBoundaries.rename(columns = {'STE_NAME21':'STEName'}, inplace = True)
SA2DigitalBoundaries.rename(columns = {'AUS_CODE21':'AUSCode'}, inplace = True)
SA2DigitalBoundaries.rename(columns = {'AUS_NAME21':'AUSName'}, inplace = True)
SA2DigitalBoundaries.rename(columns = {'AREASQKM21':'Area'}, inplace = True)

# Type Correction
SA2DigitalBoundaries['SA2code'] = pd.to_numeric( SA2DigitalBoundaries['SA2code'], errors='coerce')
SA2DigitalBoundaries = SA2DigitalBoundaries.dropna(subset=['SA2code'])
SA2DigitalBoundaries['SA2code'] = SA2DigitalBoundaries['SA2code'].astype(int)
SA2DigitalBoundaries['SA2code'] = SA2DigitalBoundaries['SA2code'].astype(int)
SA2DigitalBoundaries['SA2name'] = SA2DigitalBoundaries['SA3code'].astype(str)
SA2DigitalBoundaries['CHGFlage'] = SA2DigitalBoundaries['CHGFlage'].astype(int)
SA2DigitalBoundaries['CHGLabel'] = SA2DigitalBoundaries['CHGLabel'].astype(str)
SA2DigitalBoundaries['SA3code'] = SA2DigitalBoundaries['SA3code'].astype(int)
SA2DigitalBoundaries['SA3name'] = SA2DigitalBoundaries['SA3name'].astype(str)
SA2DigitalBoundaries['SA4code'] = SA2DigitalBoundaries['SA4code'].astype(int)
SA2DigitalBoundaries['SA4name'] = SA2DigitalBoundaries['SA4name'].astype(str)
SA2DigitalBoundaries['GCCCode'] = SA2DigitalBoundaries['GCCCode'].astype(str)
SA2DigitalBoundaries['GCCName'] = SA2DigitalBoundaries['GCCName'].astype(str)
SA2DigitalBoundaries['STECode'] = SA2DigitalBoundaries['STECode'].astype(int)
SA2DigitalBoundaries['STEName'] = SA2DigitalBoundaries['STEName'].astype(str)
SA2DigitalBoundaries['AUSCode'] = SA2DigitalBoundaries['AUSCode'].astype(str)
SA2DigitalBoundaries['AUSName'] = SA2DigitalBoundaries['AUSName'].astype(str)
SA2DigitalBoundaries['Area'] = SA2DigitalBoundaries['Area'].astype(float)

# Converting to multipolygon
SA2DigitalBoundaries['Geometry'] = SA2DigitalBoundaries['geometry'].apply(lambda x: WKTElement(x.wkt, srid=srid))
SA2DigitalBoundaries = SA2DigitalBoundaries.drop(columns="geometry")

SA2 = SA2DigitalBoundaries
credentials = "Credentials.json"

db, conn = pgconnect(credentials)
with open('Schema.sql', 'r') as file:
    schema = file.read()

pgquery(conn.execute(schema))