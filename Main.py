import os
import numpy as np
import seaborn as sns
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
        notE = "This result object does not return rows. It has been closed automatically."
        if str(e) != notE: print(e)
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
    SA2DigitalBoundaries.rename(columns={"SA2_CODE21":"sa2_code21","SA2_NAME21":"sa2_name21"}, inplace=True)
    schema = """
    DROP TABLE IF EXISTS SA2;
    CREATE TABLE SA2 (
        "sa2_code21" INTEGER,
        "sa2_name21" VARCHAR(255),
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
    CREATE TABLE School (
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
    PollingPlace = PollingPlace.drop(columns=['FID','state','division_id','polling_place_id','polling_place_type_id','premises_name','premises_address_1','premises_address_2','premises_address_3','premises_suburb','premises_state_abbreviation','premises_post_code',"the_geom"])
    PollingPlace.dropna(subset = ['longitude','latitude'],inplace=True)
    PollingPlace['geometry'] = gpd.points_from_xy(PollingPlace.longitude, PollingPlace.latitude)
    PollingPlace['geom'] = PollingPlace['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))
    PollingPlace.drop(columns=['longitude','latitude','geometry'], inplace=True)
    schema = """
    DROP TABLE IF EXISTS PollingPlace;
    CREATE TABLE PollingPlace (
        "division_name" VARCHAR(255),
        "polling_place_name" VARCHAR(255),
        geom GEOMETRY(POINT, 4326)
    );
    """
    try:
        conn.execute(text(schema))
    except Exception as e:
        print("Error executing SQL statement:", e)
    try:
        PollingPlace.to_sql("pollingplace", conn, if_exists='append', index=False, dtype={'geom': Geometry('POINT', 4326)})
    except Exception as e:
        print("Error inserting data:", e)
    print(query(conn, "select * from pollingplace"))

def importPopulation(currentDir, conn) -> None:
    PopulationPath = os.path.join(currentDir, "Data", "Population.csv")
    Population = pd.read_csv(PopulationPath)
    Population.drop_duplicates(inplace=True)
    Population.dropna(inplace=True)
    Population = Population[Population['total_people'] != 0]
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
    Stops['geometry'] = gpd.points_from_xy(Stops.stop_lon, Stops.stop_lat) 
    Stops['geom'] = Stops['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))
    Stops = Stops.drop(columns=['stop_lat', 'stop_lon','geometry','location_type','parent_station','wheelchair_boarding','platform_code'])
    Stops.drop_duplicates(inplace=True)
    Stops.dropna(inplace=True)
    schema = """
    DROP TABLE IF EXISTS Stops;
    CREATE TABLE Stops (
        "stop_id" VARCHAR(255),
        "stop_code" INTEGER,
        "stop_name" VARCHAR(255),
        "geom" GEOMETRY(POINT, 4326)
    );
    """
    try:
        conn.execute(text(schema))
    except Exception as e:
        print("Error executing SQL statement:", e)
    try:
        Stops.to_sql("stops", conn, if_exists='append', index=False, dtype={'geom': Geometry('POINT', 4326)})
    except Exception as e:
        print("Error inserting data:", e)
    print(query(conn, "select * from stops"))

def indexing(conn) -> None:
    query(conn,"""CREATE INDEX IF NOT EXISTS indexSA2 ON SA2 USING GIST("geom")""")
    query(conn,"""CREATE INDEX IF NOT EXISTS indexSA2Bussiness ON Business ("sa2_code")""")
    query(conn,"""CREATE INDEX IF NOT EXISTS indexSA2Income ON Income ("sa2_code21")""")
    query(conn,"""CREATE INDEX IF NOT EXISTS indexSA2Population ON Population ("sa2_code")""")
    query(conn,"""CREATE INDEX IF NOT EXISTS indexStops ON Stops USING GIST("geom")""")
    query(conn,"""CREATE INDEX IF NOT EXISTS indexPolling ON PollingPlace USING GIST("geom")""")

def importTrees(currentDir, conn) -> None:
    treesPath = os.path.join(currentDir, "Data", "trees.csv")
    Trees = pd.read_csv(treesPath)
    Trees.drop(columns=['OID_', 'asset_id', 'SpeciesName', 'CommonName', 'TreeHeight', 'TreeCanopyEW', 'TreeCanopyNS', 'Tree_Status', 'TreeType'], inplace=True)
    Trees['geom'] = gpd.points_from_xy(Trees.Longitude, Trees.Latitude) 
    Trees['Geometry'] = Trees['geom'].apply(lambda x: WKTElement(x.wkt, srid=4326))
    Trees = Trees.drop(columns=['Latitude', 'Longitude','geom'])
    Trees.drop_duplicates(inplace=True)
    Trees.dropna(inplace=True)
    schema = """
    DROP TABLE IF EXISTS trees;
    CREATE TABLE trees (
        "ObjectId" INTEGER,
        "Geometry" GEOMETRY(POINT, 4326)
    );
    """
    try:
        conn.execute(text(schema))
        print("Table created successfully.")
    except Exception as e:
        print("Error executing SQL statement:", e)
    try:
        Trees.to_sql("trees", conn, if_exists='append', index=False, dtype={'Geometry': Geometry('POINT', 4326)})
        print("Data inserted successfully.")
    except Exception as e:
        print("Error inserting data:", e)
    print(query(conn, "select * from trees"))

def importParking(currentDir, conn) -> None:
    parkingPath = os.path.join(currentDir, "Data", "Mobility_parking.geojson")
    Parking = gpd.read_file(parkingPath)
    Parking['Geometry'] = Parking['geometry'].apply(lambda x: create_wkt_element(geom=x, srid=4326))
    Parking.drop(columns=['geometry'], inplace=True)
    Parking.drop(['Address','Street','Location','SideOfStreet','ParkingSpaceWidth','ParkingSpaceLength','ParkingSpaceAngle','SignText','URL','AuditDate'], axis=1, inplace=True)
    Parking.drop_duplicates(inplace=True)
    Parking.dropna(inplace=True)
    schema = """
    DROP TABLE IF EXISTS parking;
    CREATE TABLE Parking (
        "OBJECTID" INTEGER,
        "SiteID" INTEGER,
        "Suburb" VARCHAR(32),
        "NumberParkingSpaces" INTEGER,
        "Geometry" GEOMETRY(POINT, 4326)
    );
    """
    try:
        conn.execute(text(schema))
    except Exception as e:
        print("Error executing SQL statement:", e)
    try:
        Parking.to_sql("parking", conn, if_exists='append', index=False, dtype={'Geometry': Geometry('POINT', 4326)})
    except Exception as e:
        print("Error inserting data:", e)
    print(query(conn, "select * from parking"))

def importStairs(currentDir, conn) -> None:
    stairsPath = os.path.join(currentDir, "Data", "stairs.geojson")
    Stairs = gpd.read_file(stairsPath)
    Stairs = Stairs.drop(columns=['ID', 'No_Steps', 'TGSI', 'StairNosingConstrastStrip','ClosestAlternateRoutes', 'Photo'])
    Stairs['Geometry'] = Stairs['geometry'].apply(lambda x: create_wkt_element(geom=x, srid=4326))
    Stairs.drop(columns=['geometry'], inplace=True)
    Stairs.drop_duplicates(inplace=True)
    Stairs.dropna(inplace=True)
    schema = """
    DROP TABLE IF EXISTS stairs;
    CREATE TABLE stairs (
        "OBJECTID" INTEGER,
        "Name" VARCHAR(255),
        "Address" VARCHAR(128),
        "Suburb" VARCHAR(64),
        "HandRails" VARCHAR(32),
        "Geometry" GEOMETRY(POINT, 4326)
    );
    """
    try:
        conn.execute(text(schema))
    except Exception as e:
        print("Error executing SQL Statement:", e)
    try:
        Stairs.to_sql("stairs", conn, if_exists='append', index=False, dtype={'Geometry': Geometry('POINT', 4326)})
    except Exception as e:
        print("Error inserting data:", e)
    print(query(conn, "select * from stairs"))

def zScore(conn):
    zbussiness = """
    DROP TABLE IF EXISTS bussTable;
    CREATE TABLE bussTable AS
    SELECT p.sa2_code, p.sa2_name,  
    (CAST(b.total_businesses AS FLOAT) / p.total_people * 1000 - AVG(CAST(b.total_businesses AS FLOAT) / p.total_people * 1000) OVER ()) / STDDEV_POP(CAST(b.total_businesses AS FLOAT) / p.total_people * 1000) OVER () AS zbusiness
    FROM Business b JOIN Population p 
    ON p.sa2_code = b.sa2_code 
    WHERE industry_name = 'Electricity, Gas, Water and Waste Services'
    """
    query(conn, zbussiness)
    print(pd.read_sql_query("SELECT * from bussTable ORDER BY zbusiness DESC;", conn))

    zStops = """
    DROP TABLE IF EXISTS stopsTable;
    CREATE TABLE stopsTable AS 
    SELECT s.SA2_CODE21, s.SA2_NAME21, 
        (COUNT(st.stop_id) - AVG(COUNT(st.stop_id)) OVER ()) / STDDEV_POP(COUNT(st.stop_id)) OVER () AS zstops
    FROM SA2 s 
    JOIN (
        SELECT stop_id, geom
        FROM Stops
    ) st ON ST_Contains(s.geom, st.geom)
    GROUP BY s.SA2_CODE21, s.SA2_NAME21;
    """
    query(conn, zStops)
    print(pd.read_sql_query("SELECT * from stopsTable ORDER BY zstops DESC;", conn))

    zPoll = """
    DROP TABLE IF EXISTS pollTable;
    CREATE TABLE pollTable AS
    SELECT s.SA2_CODE21, s.SA2_NAME21, (COUNT(p.division_name) - AVG(COUNT(p.division_name)) OVER ()) / STDDEV_POP(COUNT(p.division_name)) OVER () AS zpoll
    FROM SA2 s JOIN PollingPlace p ON ST_Contains(s.geom, p.geom)
    GROUP BY s.SA2_CODE21, s.SA2_NAME21;
    """
    query(conn, zPoll)
    print(pd.read_sql_query("SELECT * from pollTable ORDER BY zpoll DESC;", conn))

    zSchool = """
    DROP TABLE IF EXISTS helper;
    CREATE TABLE helper AS
    SELECT s.SA2_CODE21, s.SA2_NAME21, SUM(p."0-4_people" + p."5-9_people" + p."10-14_people" + p."15-19_people") AS under_19
    FROM SA2 s
    JOIN POPULATION p ON s.SA2_CODE21 = p.sa2_code
    GROUP BY s.SA2_CODE21, s.SA2_NAME21;

    DROP TABLE IF EXISTS schoolTable;
    CREATE TABLE schoolTable AS
    WITH schoolTable AS (
        SELECT s.SA2_CODE21, s.SA2_NAME21, CAST(COUNT(*) AS FLOAT) * 1000 / h.under_19 AS avg
        FROM SA2 s
        JOIN School sc ON ST_INTERSECTS(sc."Geometry", s.geom)
        JOIN helper h ON h.SA2_CODE21 = s.SA2_CODE21
        WHERE h.under_19 > 0
        GROUP BY s.SA2_CODE21, s.SA2_NAME21, h.under_19
    ),
    stats AS (
        SELECT AVG(avg) AS mean_avg, STDDEV(avg) AS stddev_avg
        FROM schoolTable
    )
    SELECT 
        sd.SA2_CODE21, sd.SA2_NAME21, sd.avg, (sd.avg - st.mean_avg) / st.stddev_avg AS zschool
    FROM schoolTable sd, stats st;
    """
    query(conn, zSchool)
    print(pd.read_sql_query("SELECT * from schoolTable ORDER BY zschool DESC;", conn))

    zTrees = """
    DROP TABLE IF EXISTS treeTable;
    CREATE TABLE treeTable AS
    SELECT s.SA2_CODE21, s.SA2_NAME21, (COUNT(t."ObjectId") - AVG(COUNT(t."ObjectId")) OVER ()) / STDDEV_POP(COUNT(t."ObjectId")) OVER () AS ztrees
    FROM SA2 s JOIN trees t ON ST_Contains(s.geom, t."Geometry")
    GROUP BY s.SA2_CODE21, s.SA2_NAME21;
    """
    query(conn, zTrees)
    print(pd.read_sql_query("SELECT * from treeTable ORDER BY ztrees DESC;", conn))
    print()

    zParking = """
    DROP TABLE IF EXISTS parkTable;
    CREATE TABLE parkTable AS
    SELECT s.SA2_CODE21, s.SA2_NAME21, (COUNT(p."OBJECTID") - AVG(COUNT(p."OBJECTID")) OVER ()) / STDDEV_POP(COUNT(p."OBJECTID")) OVER () AS zpark
    FROM SA2 s JOIN parking p ON ST_Contains(s.geom, p."Geometry")
    GROUP BY s.SA2_CODE21, s.SA2_NAME21;
    """
    query(conn, zParking)
    print(pd.read_sql_query("SELECT * from parkTable ORDER BY zpark DESC;", conn))
    print()

    zStairs = """
    DROP TABLE IF EXISTS stairsTable;
    CREATE TABLE stairsTable AS
    SELECT s.SA2_CODE21, s.SA2_NAME21, (COUNT(st."OBJECTID") - AVG(COUNT(st."OBJECTID")) OVER ()) / STDDEV_POP(COUNT(st."OBJECTID")) OVER () AS zstairs
    FROM SA2 s JOIN stairs st ON ST_Contains(s.geom, st."Geometry")
    GROUP BY s.SA2_CODE21, s.SA2_NAME21;
    """
    query(conn, zStairs)
    print(pd.read_sql_query("SELECT * from stairsTable ORDER BY zstairs DESC;", conn))
    print()

def sigmoid(conn) -> None:
    sigmodi = """
    DROP TABLE IF EXISTS zscore;
    CREATE TABLE zscore AS
    SELECT s.SA2_CODE21, s.SA2_NAME21, 
        (zb.zbusiness + zs.zstops + zp.zpoll + zsc.zschool) AS score
    FROM SA2 s
    JOIN bussTable zb ON s.SA2_CODE21 = zb.sa2_code
    JOIN stopsTable zs ON s.SA2_CODE21 = zs.sa2_code21
    JOIN pollTable zp ON s.SA2_CODE21 = zp.sa2_code21
    JOIN schoolTable zsc ON s.SA2_CODE21 = zsc.sa2_code21;
    """
    query(conn, sigmodi)
    df = pd.read_sql_query("SELECT * from zscore ORDER BY score DESC;", conn)
    df['bustling_score'] = 1 / (1 + np.exp(-df['score']))
    print(f"{df}\n")

    sigmodi = """
    DROP TABLE IF EXISTS zscore;
    CREATE TABLE zscore AS
    SELECT s.SA2_CODE21, s.SA2_NAME21, 
        (zb.zbusiness + zs.zstops + zp.zpoll + zt.ztrees + zpk.zpark + zst.zstairs + zsc.zschool) AS score
    FROM SA2 s
    JOIN bussTable zb ON s.SA2_CODE21 = zb.sa2_code
    JOIN stopsTable zs ON s.SA2_CODE21 = zs.sa2_code21
    JOIN pollTable zp ON s.SA2_CODE21 = zp.sa2_code21
    JOIN treeTable zt ON s.SA2_CODE21 = zt.sa2_code21
    JOIN parkTable zpk ON s.SA2_CODE21 = zpk.sa2_code21
    JOIN stairsTable zst ON s.SA2_CODE21 = zst.sa2_code21
    JOIN schoolTable zsc ON s.SA2_CODE21 = zsc.sa2_code21;
    """
    query(conn, sigmodi)
    df = pd.read_sql_query("SELECT * from zscore ORDER BY score DESC;", conn)
    df['bustling_score'] = 1 / (1 + np.exp(-df['score']))
    print(df)

def plotData(conn) -> None:
    query(conn,"""
    ALTER TABLE zscore ADD IF NOT EXISTS median_income FLOAT;
    UPDATE zscore
    SET median_income = Income.median_income
    FROM Income
    WHERE zscore.SA2_code21 = INCOME.SA2_code21;
    """)
    df = pd.read_sql_query(""" SELECT * from zscore;""",conn)
    plt.figure(figsize=(10, 6))
    plt.scatter(df['score'], df['median_income'], alpha=0.5)
    plt.title('Scatter Plot of  Score A vs Median Income')
    plt.xlabel('Score')
    plt.ylabel('Median Income')
    plt.grid(True)
    plt.show()
    map_overlay = gpd.read_postgis("SELECT geom, score FROM SA2 s JOIN zscore r ON (s.sa2_code21 = r.sa2_code21)", conn, geom_col='geom')
    plt.rcParams
    map_overlay.plot(column='score', cmap='GnBu', legend=True)
    plt.title('Resource Score Map Overlay')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    # plt.savefig('map.png')
    plt.show()

if __name__ == "__main__":
    credentials = "Credentials.json"
    currentDir = os.path.dirname(os.path.abspath(__file__))
    db, conn = pgconnect(credentials)

    importSA2(currentDir, conn)
    importSchool(currentDir, conn)
    importBusiness(currentDir, conn)
    importIncome(currentDir, conn)
    importPolling(currentDir, conn)
    importPopulation(currentDir, conn)
    importStops(currentDir, conn)
    importTrees(currentDir, conn)
    importParking(currentDir, conn)
    importStairs(currentDir, conn)
    zScore(conn)
    sigmoid(conn)
    plotData(conn)