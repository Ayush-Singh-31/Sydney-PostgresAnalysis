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
    elif geom.geom_type == 'Point':
        geom = Point([xy[0:2] for xy in list(geom.coords)])
    return WKTElement(geom.wkt, srid)

def importSA2(currentDir, conn) -> None:
    SA2DigitalBoundariesPath = os.path.join(currentDir, "Data", "SA2 Digital Boundaries","SA2_2021_AUST_GDA2020.shp")
    SA2DigitalBoundaries = gpd.read_file(SA2DigitalBoundariesPath)
    SA2DigitalBoundaries.dropna(subset=['geometry'], inplace=True)
    SA2DigitalBoundaries = SA2DigitalBoundaries[SA2DigitalBoundaries['GCC_NAME21'] == 'Greater Sydney'].copy()
    SA2DigitalBoundaries['geom'] = SA2DigitalBoundaries['geometry'].apply(lambda x: create_wkt_element(geom=x,srid=4326))
    SA2DigitalBoundaries = SA2DigitalBoundaries.drop(columns="geometry")
    SA2DigitalBoundaries.drop(columns=['LOCI_URI21','CHG_FLAG21','CHG_LBL21','SA3_CODE21','SA3_NAME21','SA4_CODE21','SA4_NAME21',"GCC_CODE21", "GCC_NAME21", "STE_CODE21" ,  "STE_NAME21" , "AUS_CODE21", "AUS_NAME21"], inplace=True)
    SA2DigitalBoundaries.drop_duplicates(inplace=True)
    SA2DigitalBoundaries.dropna(inplace=True)
    schema = """
    DROP TABLE IF EXISTS SA2;
    CREATE TABLE SA2 (
        "SA2_CODE21" INTEGER,
        "SA2_NAME21" VARCHAR(255),
        "AREASQKM21" FLOAT,
        "geom" GEOMETRY(MULTIPOLYGON, 4326)
    );
    """
    try:
        conn.execute(text(schema))
    except Exception as e:
        print("Error executing SQL statement:", e)
    try:
        SA2DigitalBoundaries.to_sql("sa2", conn, if_exists='append', index=False, dtype={'geom': Geometry('MULTIPOLYGON', 4326)})
    except Exception as e:
        print("Error inserting data:", e)
    print(query(conn, "select * from SA2"))

def importSchool(currentDir, conn) -> None:
    CatchmentPrimaryPath = os.path.join(currentDir, "Data", "catchments", "catchments_primary.shp")
    CatchmentPrimary = gpd.read_file(CatchmentPrimaryPath)
    CatchmentPrimary['Geometry'] = CatchmentPrimary['geometry'].apply(lambda x: create_wkt_element(geom=x, srid=4326))
    CatchmentPrimary = CatchmentPrimary.drop(columns="geometry")
    CatchmentSecondaryPath = os.path.join(currentDir, "Data", "catchments", "catchments_secondary.shp")
    CatchmentSecondary = gpd.read_file(CatchmentSecondaryPath)
    CatchmentSecondary['Geometry'] = CatchmentSecondary['geometry'].apply(lambda x: create_wkt_element(geom=x, srid=4326))
    CatchmentSecondary = CatchmentSecondary.drop(columns="geometry")

    school = pd.concat([CatchmentPrimary, CatchmentSecondary])
    school.drop_duplicates(inplace=True)
    school.drop(columns=[ 'ADD_DATE', 'KINDERGART', 'YEAR1','YEAR2', 'YEAR3', 'YEAR4', 'YEAR5', 'YEAR6', 'YEAR7', 'YEAR8', 'YEAR9','YEAR10', 'YEAR11', 'YEAR12', 'PRIORITY'], inplace=True)
    school.dropna(inplace=True)
    schema = """
    DROP TABLE IF EXISTS School;
    CREATE TABLE CatchmentCombined (
        "USE_ID" INTEGER,
        "CATCH_TYPE" VARCHAR(255),
        "USE_DESC" VARCHAR(255),
        "Geometry" GEOMETRY(MULTIPOLYGON, 4326)
    );
    """
    try:
        conn.execute(text(schema))
    except Exception as e:
        print("Error executing SQL statement:", e)
    try:
        school.to_sql("school", conn, if_exists='append', index=False, dtype={'Geometry': Geometry('MULTIPOLYGON', 4326)})
    except Exception as e:
        print("Error inserting data:", e)
    print(query(conn, "select * from school"))

def importBusiness(currentDir, conn) -> None:
    BusinessPath = os.path.join(currentDir, "Data", "Businesses.csv")
    Business = pd.read_csv(BusinessPath)
    Business.drop(columns=['industry_code',], inplace=True)
    Business.drop_duplicates(inplace=True)
    Business.dropna(inplace=True)
    schema = """
    DROP TABLE IF EXISTS Business;
    CREATE TABLE Business (
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
    except Exception as e:
        print("Error executing SQL statement:", e)
    try:
        Business.to_sql("business", conn, if_exists='append', index=False)
    except Exception as e:
        print("Error inserting data:", e)
    print(query(conn, "select * from business"))

def importIncome(currentDir, conn) -> None:
    IncomePath = os.path.join(currentDir, "Data", "Income.csv")
    Income = pd.read_csv(IncomePath)
    Income.drop_duplicates(inplace=True)
    Income.dropna(inplace=True)
    Income.replace('np', 0, inplace=True)
    Income['earners'] = Income['earners'].astype(int)
    Income['median_age'] = Income['median_age'].astype(int)
    Income['median_income'] = Income['median_income'].astype(int)
    Income['mean_income'] = Income['mean_income'].astype(int)
    schema = """
    DROP TABLE IF EXISTS Income;
    CREATE TABLE Income (
        "sa2_code21" INTEGER,
        "sa2_name" VARCHAR(255),
        "earners" INTEGER,
        "median_age" INTEGER,
        "median_income" INTEGER,
        "mean_income" INTEGER
    );
    """
    try:
        conn.execute(text(schema))
    except Exception as e:
        print("Error executing SQL statement:", e)
    try:
        Income.to_sql("income", conn, if_exists='append', index=False)
    except Exception as e:
        print("Error inserting data:", e)
    print(query(conn, "select * from income"))

def importPolling(currentDir, conn) -> None:
    PollingPlacesPath = os.path.join(currentDir, "Data", "PollingPlaces2019.csv")
    PollingPlace = pd.read_csv(PollingPlacesPath)
    PollingPlace.drop_duplicates(inplace=True)
    PollingPlace.drop(columns=['longitude','latitude','state','division_id','division_name','polling_place_id','polling_place_type_id','premises_address_1','premises_address_2','premises_address_3','premises_suburb','premises_state_abbreviation','premises_post_code'], inplace=True)
    PollingPlace.dropna(inplace=True)
    schema = """
    DROP TABLE IF EXISTS PollingPlace;
    CREATE TABLE PollingPlace (
        "FID" VARCHAR(255),
        "polling_place_name" VARCHAR(255),
        "premises_name" VARCHAR(255),
        "the_geom" GEOMETRY(POINT, 4326)
    );
    """
    try:
        conn.execute(text(schema))
    except Exception as e:
        print("Error executing SQL statement:", e)
    try:
        PollingPlace.to_sql("pollingplace", conn, if_exists='append', index=False, dtype={'Geometry': Geometry('POINT', 4326)})
    except Exception as e:
        print("Error inserting data:", e)
    print(query(conn, "select * from pollingplace"))

def importPolpulation(currentDir, conn) -> None:
    PopulationPath = os.path.join(currentDir, "Data", "Population.csv")
    Population = pd.read_csv(PopulationPath)
    Population.drop_duplicates(inplace=True)
    Population.dropna(inplace=True)
    schema = """
    DROP TABLE IF EXISTS Population;
    CREATE TABLE Population (
        "sa2_code" INTEGER,
        "sa2_name" VARCHAR(255),
        "0-4_people" INTEGER,
        "5-9_people" INTEGER,
        "10-14_people" INTEGER,
        "15-19_people" INTEGER,
        "20-24_people" INTEGER,
        "25-29_people" INTEGER,
        "30-34_people" INTEGER,
        "35-39_people" INTEGER,
        "40-44_people" INTEGER,
        "45-49_people" INTEGER,
        "50-54_people" INTEGER,
        "55-59_people" INTEGER,
        "60-64_people" INTEGER,
        "65-69_people" INTEGER,
        "70-74_people" INTEGER,
        "75-79_people" INTEGER,
        "80-84_people" INTEGER,
        "85-and-over_people" INTEGER,
        "total_people" INTEGER
    );
    """
    try:
        conn.execute(text(schema))
    except Exception as e:
        print("Error executing SQL statement:", e)
    try:
        Population.to_sql("population", conn, if_exists='append', index=False)
    except Exception as e:
        print("Error inserting data:", e)
    print(query(conn, "select * from population"))

def importStops(currentDir, conn) -> None:
    stopsPath = os.path.join(currentDir, "Data", "Stops.txt")
    Stops = pd.read_csv(stopsPath)
    Stops['platform_code'] = pd.to_numeric(Stops['platform_code'], errors='coerce').fillna(0).astype(int)
    Stops['parent_station'] = pd.to_numeric(Stops['parent_station'], errors='coerce').fillna(0).astype(int)
    Stops['location_type'] = pd.to_numeric(Stops['location_type'], errors='coerce').fillna(0).astype(int)
    Stops['stop_code'] = pd.to_numeric(Stops['stop_code'], errors='coerce').fillna(0).astype(int)
    Stops['geom'] = gpd.points_from_xy(Stops.stop_lon, Stops.stop_lat) 
    Stops['Geometry'] = Stops['geom'].apply(lambda x: WKTElement(x.wkt, srid=4326))
    Stops = Stops.drop(columns=['stop_lat', 'stop_lon','geom','location_type','parent_station','wheelchair_boarding','platform_code'])
    Stops.drop_duplicates(inplace=True)
    Stops.dropna(inplace=True)
    schema = """
    DROP TABLE IF EXISTS Stops;
    CREATE TABLE Stops (
        "stop_id" VARCHAR(255),
        "stop_code" INTEGER,
        "stop_name" VARCHAR(255),
        "Geometry" GEOMETRY(POINT, 4326)
    );
    """
    try:
        conn.execute(text(schema))
    except Exception as e:
        print("Error executing SQL statement:", e)
    try:
        Stops.to_sql("stops", conn, if_exists='append', index=False, dtype={'Geometry': Geometry('POINT', 4326)})
    except Exception as e:
        print("Error inserting data:", e)
    print(query(conn, "select * from stops"))

if __name__ == "__main__":
    credentials = "Credentials.json"
    currentDir = os.path.dirname(os.path.abspath(__file__))
    db, conn = pgconnect(credentials)

    importSA2(currentDir, conn)
    importSchool(currentDir, conn)
    importBusiness(currentDir, conn)
    importIncome(currentDir, conn)
    importPolling(currentDir, conn)
    importPolpulation(currentDir, conn)
    importStops(currentDir, conn)