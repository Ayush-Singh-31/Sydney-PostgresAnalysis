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

def create_wkt_element(geom, srid):
    if geom.geom_type == 'Polygon':
        geom = MultiPolygon([geom])
    return WKTElement(geom.wkt, srid)

def importSA2(currentDir, conn) -> None:
    SA2DigitalBoundariesPath = os.path.join(currentDir, "Data", "SA2 Digital Boundaries","SA2_2021_AUST_GDA2020.shp")
    SA2DigitalBoundaries = gpd.read_file(SA2DigitalBoundariesPath)
    SA2DigitalBoundaries.dropna(subset=['geometry'], inplace=True)
    SA2DigitalBoundaries['geom'] = SA2DigitalBoundaries['geometry'].apply(lambda x: create_wkt_element(geom=x,srid=4326))
    SA2DigitalBoundaries = SA2DigitalBoundaries.drop(columns="geometry")
    SA2DigitalBoundaries.drop(columns=['LOCI_URI21'], inplace=True)
    SA2DigitalBoundaries.drop_duplicates()
    schema = """
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
        conn.execute(text(schema))
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

def importCPrimary(currentDir, conn) -> None:
    CatchmentPrimaryPath = os.path.join(currentDir, "Data", "catchments", "catchments_primary.shp")
    CatchmentPrimary = gpd.read_file(CatchmentPrimaryPath)
    CatchmentPrimary['Geometry'] = CatchmentPrimary['geometry'].apply(lambda x: create_wkt_element(geom=x,srid=4326))
    CatchmentPrimary = CatchmentPrimary.drop(columns="geometry")
    CatchmentPrimary.drop_duplicates()
    print(CatchmentPrimary.head())
    schema = """
    DROP TABLE IF EXISTS CatchmentPrimary;
    CREATE TABLE CatchmentPrimary (
        "USE_ID" INTEGER PRIMARY KEY,
        "CATCH_TYPE" VARCHAR(255),
        "USE_DESC" VARCHAR(255),
        "ADD_DATE" DATE,
        "KINDERGART" BOOLEAN,
        "YEAR1" BOOLEAN,
        "YEAR2" BOOLEAN,
        "YEAR3" BOOLEAN,
        "YEAR4" BOOLEAN,
        "YEAR5" BOOLEAN,
        "YEAR6" BOOLEAN,
        "YEAR7" BOOLEAN,
        "YEAR8" BOOLEAN,
        "YEAR9" BOOLEAN,
        "YEAR10" BOOLEAN,
        "YEAR11" BOOLEAN,
        "YEAR12" BOOLEAN,
        "PRIORITY" BOOLEAN,
        "Geometry" GEOMETRY(MULTIPOLYGON, 4326)
    );
    """
    try:
        conn.execute(text(schema))
        print("Table created successfully.")
    except Exception as e:
        print("Error executing SQL statement:", e)
    try:
        CatchmentPrimary.to_sql("catchmentprimary", conn, if_exists='append', index=False, dtype={'Geometry': Geometry('MULTIPOLYGON', 4326)})
        print("Data inserted successfully.")
    except Exception as e:
        print("Error inserting data:", e)
    print(query(conn, "select * from catchmentprimary"))

def importCSecondary(currentDir, conn) -> None:
    CatchmentSecondaryPath = os.path.join(currentDir, "Data", "catchments", "catchments_secondary.shp")
    CatchmentSecondary = gpd.read_file(CatchmentSecondaryPath)
    CatchmentSecondary['Geometry'] = CatchmentSecondary['geometry'].apply(lambda x: create_wkt_element(geom=x,srid=4326))
    CatchmentSecondary = CatchmentSecondary.drop(columns="geometry")
    CatchmentSecondary.drop_duplicates()
    print(CatchmentSecondary.head())
    schema = """
    DROP TABLE IF EXISTS CatchmentSecondary;
    CREATE TABLE CatchmentSecondary (
        "USE_ID" INTEGER PRIMARY KEY,
        "CATCH_TYPE" VARCHAR(255),
        "USE_DESC" VARCHAR(255),
        "ADD_DATE" DATE,
        "KINDERGART" BOOLEAN,
        "YEAR1" BOOLEAN,
        "YEAR2" BOOLEAN,
        "YEAR3" BOOLEAN,
        "YEAR4" BOOLEAN,
        "YEAR5" BOOLEAN,
        "YEAR6" BOOLEAN,
        "YEAR7" BOOLEAN,
        "YEAR8" BOOLEAN,
        "YEAR9" BOOLEAN,
        "YEAR10" BOOLEAN,
        "YEAR11" BOOLEAN,
        "YEAR12" BOOLEAN,
        "PRIORITY" BOOLEAN,
        "Geometry" GEOMETRY(MULTIPOLYGON, 4326)
    );
    """
    try:
        conn.execute(text(schema))
        print("Table created successfully.")
    except Exception as e:
        print("Error executing SQL statement:", e)
    try:
        CatchmentSecondary.to_sql("catchmentsecondary", conn, if_exists='append', index=False, dtype={'Geometry': Geometry('MULTIPOLYGON', 4326)})
        print("Data inserted successfully.")
    except Exception as e:
        print("Error inserting data:", e)
    print(query(conn, "select * from catchmentsecondary"))

def importCFuture(currentDir, conn) -> None:
    CatchmentFuturePath = os.path.join(currentDir, "Data", "catchments", "catchments_future.shp")
    CatchmentFuture = gpd.read_file(CatchmentFuturePath)
    CatchmentFuture['Geometry'] = CatchmentFuture['geometry'].apply(lambda x: create_wkt_element(geom=x,srid=4326))
    CatchmentFuture = CatchmentFuture.drop(columns="geometry")
    CatchmentFuture.drop_duplicates()
    CatchmentFuture['KINDERGART'] = CatchmentFuture['KINDERGART'].astype(bool)
    CatchmentFuture['YEAR1'] = CatchmentFuture['YEAR1'].astype(bool)
    CatchmentFuture['YEAR2'] = CatchmentFuture['YEAR2'].astype(bool)
    CatchmentFuture['YEAR3'] = CatchmentFuture['YEAR3'].astype(bool)
    CatchmentFuture['YEAR4'] = CatchmentFuture['YEAR4'].astype(bool)
    CatchmentFuture['YEAR5'] = CatchmentFuture['YEAR5'].astype(bool)
    CatchmentFuture['YEAR6'] = CatchmentFuture['YEAR6'].astype(bool)
    CatchmentFuture['YEAR7'] = CatchmentFuture['YEAR7'].astype(bool)
    CatchmentFuture['YEAR8'] = CatchmentFuture['YEAR8'].astype(bool)
    CatchmentFuture['YEAR9'] = CatchmentFuture['YEAR9'].astype(bool)
    CatchmentFuture['YEAR10'] = CatchmentFuture['YEAR10'].astype(bool)
    CatchmentFuture['YEAR11'] = CatchmentFuture['YEAR11'].astype(bool)
    CatchmentFuture['YEAR12'] = CatchmentFuture['YEAR12'].astype(bool)
    print(CatchmentFuture.head())
    schema = """
    DROP TABLE IF EXISTS CatchmentFuture;
    CREATE TABLE CatchmentFuture (
        "USE_ID" INTEGER PRIMARY KEY,
        "CATCH_TYPE" VARCHAR(255),
        "USE_DESC" VARCHAR(255),
        "ADD_DATE" DATE,
        "KINDERGART" BOOLEAN,
        "YEAR1" BOOLEAN,
        "YEAR2" BOOLEAN,
        "YEAR3" BOOLEAN,
        "YEAR4" BOOLEAN,
        "YEAR5" BOOLEAN,
        "YEAR6" BOOLEAN,
        "YEAR7" BOOLEAN,
        "YEAR8" BOOLEAN,
        "YEAR9" BOOLEAN,
        "YEAR10" BOOLEAN,
        "YEAR11" BOOLEAN,
        "YEAR12" BOOLEAN,
        "Geometry" GEOMETRY(MULTIPOLYGON, 4326)
    );
    """
    try:
        conn.execute(text(schema))
        print("Table created successfully.")
    except Exception as e:
        print("Error executing SQL statement:", e)
    try:
        CatchmentFuture.to_sql("catchmentfuture", conn, if_exists='append', index=False, dtype={'Geometry': Geometry('MULTIPOLYGON', 4326)})
        print("Data inserted successfully.")
    except Exception as e:
        print("Error inserting data:", e)
    print(query(conn, "select * from catchmentfuture"))

def importBusiness(currentDir, Conn) -> None:
    BusinessPath = os.path.join(currentDir, "Data", "Businesses.csv")
    Business = pd.read_csv(BusinessPath)
    Business.drop_duplicates()
    print(Business.head())
    schema = """
    DROP TABLE IF EXISTS Business;
    CREATE TABLE Business (
        "industry_code" VARCHAR,
        "industry_name" VARCHAR(255),
        "sa2_code" INTEGER,
        "sa2_name" VARCHAR(255),
        "0_to_50k_businesses" INTEGER,
        "50k_to_200k_businesses" INTEGER,
        "200k_to_2m_businesses" INTEGER,
        "2m_to_5m_businesses" INTEGER,
        "5m_to_10m_businesses" INTEGER,
        "10m_or_more_businesses" INTEGER,
        "total_businesses" INTEGER
    );
    """
    try:
        conn.execute(text(schema))
        print("Table created successfully.")
    except Exception as e:
        print("Error executing SQL statement:", e)
    try:
        Business.to_sql("business", conn, if_exists='append', index=False)
        print("Data inserted successfully.")
    except Exception as e:
        print("Error inserting data:", e)
    print(query(conn, "select * from business"))

def importIncome(currentDir, conn) -> None:
    IncomePath = os.path.join(currentDir, "Data", "Income.csv")
    Income = pd.read_csv(IncomePath)
    Income.drop_duplicates()
    print(Income.head())

if __name__ == "__main__":
    credentials = "Credentials.json"
    currentDir = os.path.dirname(os.path.abspath(__file__))
    db, conn = pgconnect(credentials)

    #importSA2(currentDir, conn)

    #importCPrimary(currentDir, conn)

    #importCSecondary(currentDir, conn)

    #importCFuture(currentDir, conn)

    #importBusiness(currentDir, conn)

    importIncome(currentDir, conn)