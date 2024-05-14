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
    CatchmentPrimary['PRIORITY'] = pd.to_numeric(CatchmentPrimary['PRIORITY'], errors='coerce').fillna(0).astype(int)
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
        "PRIORITY" INTEGER,
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
    CatchmentSecondary['PRIORITY'] = pd.to_numeric(CatchmentSecondary['PRIORITY'], errors='coerce').fillna(0).astype(int)
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
        "PRIORITY" INTEGER,
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

def importSchool(currentDir, conn) -> None:
    # Import CatchmentPrimary
    CatchmentPrimaryPath = os.path.join(currentDir, "Data", "catchments", "catchments_primary.shp")
    CatchmentPrimary = gpd.read_file(CatchmentPrimaryPath)
    CatchmentPrimary['Geometry'] = CatchmentPrimary['geometry'].apply(lambda x: create_wkt_element(geom=x, srid=4326))
    CatchmentPrimary = CatchmentPrimary.drop(columns="geometry")
    CatchmentPrimary['PRIORITY'] = pd.to_numeric(CatchmentPrimary['PRIORITY'], errors='coerce').fillna(0).astype(int)

    # Import CatchmentSecondary
    CatchmentSecondaryPath = os.path.join(currentDir, "Data", "catchments", "catchments_secondary.shp")
    CatchmentSecondary = gpd.read_file(CatchmentSecondaryPath)
    CatchmentSecondary['Geometry'] = CatchmentSecondary['geometry'].apply(lambda x: create_wkt_element(geom=x, srid=4326))
    CatchmentSecondary = CatchmentSecondary.drop(columns="geometry")
    CatchmentSecondary['PRIORITY'] = pd.to_numeric(CatchmentSecondary['PRIORITY'], errors='coerce').fillna(0).astype(int)

    # Import CatchmentFuture
    CatchmentFuturePath = os.path.join(currentDir, "Data", "catchments", "catchments_future.shp")
    CatchmentFuture = gpd.read_file(CatchmentFuturePath)
    CatchmentFuture['Geometry'] = CatchmentFuture['geometry'].apply(lambda x: create_wkt_element(geom=x, srid=4326))
    CatchmentFuture = CatchmentFuture.drop(columns="geometry")
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

    # Merge Dataframes
    combined_df = pd.concat([CatchmentPrimary, CatchmentSecondary, CatchmentFuture])

    # Create Database Table
    schema = """
    DROP TABLE IF EXISTS CatchmentCombined;
    CREATE TABLE CatchmentCombined (
        "USE_ID" INTEGER,
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
        "PRIORITY" INTEGER,
        "Geometry" GEOMETRY(MULTIPOLYGON, 4326)
    );
    """
    try:
        conn.execute(text(schema))
        print("Table created successfully.")
    except Exception as e:
        print("Error executing SQL statement:", e)

    # Insert Data into Database
    try:
        combined_df.to_sql("catchmentcombined", conn, if_exists='append', index=False, dtype={'Geometry': Geometry('MULTIPOLYGON', 4326)})
        print("Data inserted successfully.")
    except Exception as e:
        print("Error inserting data:", e)

    print(query(conn, "select * from catchmentcombined"))


def importBusiness(currentDir, conn) -> None:
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

    Income.replace('np', 0, inplace=True)
    Income['earners'] = Income['earners'].astype(int)
    Income['median_age'] = Income['median_age'].astype(int)
    Income['median_income'] = Income['median_income'].astype(int)
    Income['mean_income'] = Income['mean_income'].astype(int)

    print(Income.info())
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
        print("Table created successfully.")
    except Exception as e:
        print("Error executing SQL statement:", e)
    try:
        Income.to_sql("income", conn, if_exists='append', index=False)
        print("Data inserted successfully.")
    except Exception as e:
        print("Error inserting data:", e)
    print(query(conn, "select * from income"))

def importPolling(currentDir, conn) -> None:
    PollingPlacesPath = os.path.join(currentDir, "Data", "PollingPlaces2019.csv")
    PollingPlace = pd.read_csv(PollingPlacesPath)
    PollingPlace['premises_post_code'] = pd.to_numeric(PollingPlace['premises_post_code'], errors='coerce').fillna(0).astype(int)
    PollingPlace.drop_duplicates()
    PollingPlace.drop(columns=['longitude','latitude'], inplace=True)
    PollingPlace['the_geom'] = PollingPlace['the_geom'].fillna('POINT (0 0)')
    print(PollingPlace.head())
    schema = """
    DROP TABLE IF EXISTS PollingPlace;
    CREATE TABLE PollingPlace (
        "FID" VARCHAR(255),
        "state" VARCHAR(255),
        "division_id" INTEGER,
        "division_name" VARCHAR(255),
        "polling_place_id" INTEGER,
        "polling_place_type_id" FLOAT,
        "polling_place_name" VARCHAR(255),
        "premises_name" VARCHAR(255),
        "premises_address_1" VARCHAR(255),
        "premises_address_2" VARCHAR(255),
        "premises_address_3" VARCHAR(255),
        "premises_suburb" VARCHAR(255),
        "premises_state_abbreviation" VARCHAR(255),
        "premises_post_code" INTEGER,
        "the_geom" GEOMETRY(POINT, 4326)
    );
    """
    try:
        conn.execute(text(schema))
        print("Table created successfully.")
    except Exception as e:
        print("Error executing SQL statement:", e)
    try:
        PollingPlace.to_sql("pollingplace", conn, if_exists='append', index=False, dtype={'Geometry': Geometry('POINT', 4326)})
        print("Data inserted successfully.")
    except Exception as e:
        print("Error inserting data:", e)
    print(query(conn, "select * from pollingplace"))

def importPolpulation(currentDir, conn) -> None:
    PopulationPath = os.path.join(currentDir, "Data", "Population.csv")
    Population = pd.read_csv(PopulationPath)
    Population.drop_duplicates()
    print(Population.head())
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
        print("Table created successfully.")
    except Exception as e:
        print("Error executing SQL statement:", e)
    try:
        Population.to_sql("population", conn, if_exists='append', index=False)
        print("Data inserted successfully.")
    except Exception as e:
        print("Error inserting data:", e)
    print(query(conn, "select * from population"))

def importStops(currentDir, conn) -> None:
    stopsPath = os.path.join(currentDir, "Data", "Stops.txt")
    Stops = pd.read_csv(stopsPath)
    Stops.drop_duplicates()
    Stops['platform_code'] = pd.to_numeric(Stops['platform_code'], errors='coerce').fillna(0).astype(int)
    Stops['parent_station'] = pd.to_numeric(Stops['parent_station'], errors='coerce').fillna(0).astype(int)
    Stops['location_type'] = pd.to_numeric(Stops['location_type'], errors='coerce').fillna(0).astype(int)
    Stops['stop_code'] = pd.to_numeric(Stops['stop_code'], errors='coerce').fillna(0).astype(int)
    Stops['geom'] = gpd.points_from_xy(Stops.stop_lon, Stops.stop_lat) 
    Stops['Geometry'] = Stops['geom'].apply(lambda x: WKTElement(x.wkt, srid=4326))
    Stops = Stops.drop(columns=['stop_lat', 'stop_lon','geom'])
    print(Stops.head())
    schema = """
    DROP TABLE IF EXISTS Stops;
    CREATE TABLE Stops (
        "stop_id" VARCHAR(255) PRIMARY KEY,
        "stop_code" INTEGER,
        "stop_name" VARCHAR(255),
        "location_type" INTEGER,
        "parent_station" INTEGER,
        "wheelchair_boarding" INTEGER,
        "platform_code" INTEGER,
        "Geometry" GEOMETRY(POINT, 4326)
    );
    """
    try:
        conn.execute(text(schema))
        print("Table created successfully.")
    except Exception as e:
        print("Error executing SQL statement:", e)
    try:
        Stops.to_sql("stops", conn, if_exists='append', index=False, dtype={'Geometry': Geometry('POINT', 4326)})
        print("Data inserted successfully.")
    except Exception as e:
        print("Error inserting data:", e)
    print(query(conn, "select * from stops"))

if __name__ == "__main__":
    credentials = "Credentials.json"
    currentDir = os.path.dirname(os.path.abspath(__file__))
    db, conn = pgconnect(credentials)

    # importSA2(currentDir, conn)
    # importSchool(currentDir, conn)
    # importBusiness(currentDir, conn)
    # importIncome(currentDir, conn)
    # importPolling(currentDir, conn)
    # importPolpulation(currentDir, conn)
    # importStops(currentDir, conn)
    importSchool(currentDir, conn)