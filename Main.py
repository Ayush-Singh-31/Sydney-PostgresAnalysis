import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon, MultiPolygon
from geoalchemy2 import Geometry, WKTElement
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text
import psycopg2
import psycopg2.extras
import json

credentials = "Credentials.json"

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

def readGeospatial(path) -> gpd.GeoDataFrame:
    return gpd.read_file(path)

def create_wkt_element(geom, srid):
    if geom.geom_type == 'Polygon':
        geom = MultiPolygon([geom])
    return WKTElement(geom.wkt, srid)

if __name__ == "__main__":
    currentDir = os.path.dirname(os.path.abspath(__file__))
    CatchmentPrimaryPath = os.path.join(currentDir, "Data", "catchments", "catchments_primary.shp")
    CatchmentSecondaryPath = os.path.join(currentDir, "Data", "catchments", "catchments_secondary.shp")
    CatchmentFuturePath = os.path.join(currentDir, "Data", "catchments", "catchments_future.shp")
    SA2DigitalBoundariesPath = os.path.join(currentDir, "Data", "SA2 Digital Boundaries","SA2_2021_AUST_GDA2020.shp")
    CatchmentPrimary = readGeospatial(CatchmentPrimaryPath)
    CatchmentSecondary = readGeospatial(CatchmentSecondaryPath)
    CatchmentFuture = readGeospatial(CatchmentFuturePath)
    SA2DigitalBoundaries = readGeospatial(SA2DigitalBoundariesPath)
    
    db, conn = pgconnect(credentials)
    print(query(conn, "select PostGIS_Version()"))
    SA2DigitalBoundaries.dropna(subset=['geometry'], inplace=True)
    SA2DigitalBoundaries['geom'] = SA2DigitalBoundaries['geometry'].apply(lambda x: create_wkt_element(geom=x,srid=4326))
    SA2DigitalBoundaries = SA2DigitalBoundaries.drop(columns="geometry")
    SA2DigitalBoundaries.drop(columns=['LOCI_URI21'], inplace=True)
    sql_statement = """
    DROP TABLE IF EXISTS SA2;
    CREATE TABLE SA2 (
        "SA2_CODE21" INTEGER PRIMARY KEY,
        "SA2_NAME21" VARCHAR(255),
        "CHG_FLAG21" INTEGER,
        "CHG_LBL21" VARCHAR(255),
        "SA3_CODE21" INTEGER,
        "SA3_NAME21" VARCHAR(255),
        "SA4_CODE21" INTEGER,
        "SA4_NAME21" VARCHAR(255),
        "GCC_CODE21" VARCHAR(255),
        "GCC_NAME21" VARCHAR(255),
        "STE_CODE21" INTEGER,
        "STE_NAME21" VARCHAR(255),
        "AUS_CODE21" VARCHAR(255),
        "AUS_NAME21" VARCHAR(255),
        "AREASQKM21" FLOAT,
        "geom" GEOMETRY(MULTIPOLYGON, 4326)
    );
    """

    try:
        conn.execute(text(sql_statement))
        print("Table SA2 created successfully.")
    except Exception as e:
        print("Error executing SQL statement:", e)
    print(SA2DigitalBoundaries.head())
    try:
        SA2DigitalBoundaries.to_sql("sa2", conn, if_exists='append', index=False, dtype={'geom': Geometry('MULTIPOLYGON', 4326)})
        print("Data inserted successfully.")
    except Exception as e:
        print("Error inserting data:", e)

    print(query(conn, "select * from SA2"))
