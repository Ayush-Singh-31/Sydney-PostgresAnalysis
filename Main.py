import os
import json
import numpy as np
import pandas as pd
import seaborn as sns
import geopandas as gpd
import psycopg2
import matplotlib.pyplot as plt
from geoalchemy2 import WKTElement
from shapely.geometry import MultiPolygon
from sqlalchemy import create_engine, text

def readCSV(csv) -> pd.DataFrame:
    return pd.read_csv(csv)

def readGeospatial(path) -> gpd.GeoDataFrame:
    return gpd.read_file(path)

def describeData(df, output_file) -> None:
    with open(output_file, 'w') as f:
        f.write("Head:\n")
        f.write(df.head().to_string() + '\n\n')
        f.write("Info:\n")
        df.info(buf=f)
        f.write('\n\n')
        f.write("Description:\n")
        f.write(df.describe().to_string() + '\n\n')
        f.write("Columns:\n")
        f.write(str(df.columns.tolist()) + '\n\n')
        f.write("Shape:\n")
        f.write(str(df.shape) + '\n\n')
        f.write("Data Types:\n")
        f.write(str(df.dtypes) + '\n\n')
        f.write("Missing Values:\n")
        f.write(str(df.isnull().sum()) + '\n\n')
    return None

def pairPlots(Business, Income, PollingPlace, Population, Stops):
    # Checking Outliers
    sns.pairplot(Business)
    plt.savefig('Plots/Business.png')
    sns.pairplot(Income)
    plt.savefig('Plots/Income.png')
    sns.pairplot(PollingPlace)
    plt.savefig('Plots/PollingPlace.png')
    sns.pairplot(Population)
    plt.savefig('Plots/Population.png')
    sns.pairplot(Stops)
    plt.savefig('Plots/Stops.png')

def cleanCSV(Business, Income, PollingPlace, Population, Stops):

    Business = Business.drop_duplicates() 
    Income = Income.drop_duplicates()
    PollingPlace = PollingPlace.drop_duplicates()
    Population = Population.drop_duplicates()
    Stops = Stops.drop_duplicates()
    Stops.rename(columns = {'stop_id':'StopID'}, inplace = True)
    Stops.rename(columns = {'stop_code':'StopCode'}, inplace = True)
    Stops.rename(columns = {'stop_name':'StopName'}, inplace = True)
    Stops.rename(columns = {'stop_lat':'Latitude'}, inplace = True)
    Stops.rename(columns = {'stop_lon':'Longitude'}, inplace = True)
    Stops.rename(columns = {'location_type':'LocationType'}, inplace = True)
    Stops.rename(columns = {'parent_station':'ParentStation'}, inplace = True)
    Stops.rename(columns = {'wheelchair_boarding':'WheelchairBoarding'}, inplace = True)
    Stops.rename(columns = {'platform_code':'PlatformCode'}, inplace = True)

    return Business, Income, PollingPlace, Population, Stops

def cleanGeospatial(CatchmentPrimary, CatchmentSecondary, CatchmentFuture, SA2DigitalBoundaries, StopsCSV, srid):

    SA2DigitalBoundaries.drop(columns=['LOCI_URI21'], inplace=True) 
    SA2DigitalBoundaries = SA2DigitalBoundaries[SA2DigitalBoundaries['GCC_NAME21'] == 'Greater Sydney'].copy()
    SA2DigitalBoundaries['Geometry'] = SA2DigitalBoundaries['geometry'].apply(lambda x: x.wkt if x is not None else x)
    SA2DigitalBoundaries = SA2DigitalBoundaries.drop(columns="geometry")
    
    CatchmentPrimary['Geom'] = CatchmentPrimary['geometry'].apply(lambda x: create_wkt_element(geom=x,srid=srid))  
    CatchmentFuture['Geom'] = CatchmentFuture['geometry'].apply(lambda x: create_wkt_element(geom=x,srid=srid))  
    CatchmentSecondary['Geom'] = CatchmentSecondary['geometry'].apply(lambda x: create_wkt_element(geom=x,srid=srid))  
    StopsCSV['Geometry'] = gpd.points_from_xy(StopsCSV.Latitude, StopsCSV.Longitude)  
    StopsCSV['Geom'] = StopsCSV['Geometry'].apply(lambda x: WKTElement(x.wkt, srid=srid))

    CatchmentSecondary = CatchmentSecondary.drop(columns="geometry") 
    CatchmentFuture = CatchmentFuture.drop(columns="geometry") 
    CatchmentPrimary = CatchmentPrimary.drop(columns="geometry")  
    StopsCSV = StopsCSV.drop(columns=['Latitude', 'Longitude'])

    return CatchmentPrimary, CatchmentSecondary, CatchmentFuture, SA2DigitalBoundaries, StopsCSV

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
        result = None
    return result

if __name__ == "__main__":
    srid = 4326
    currentDir = os.path.dirname(os.path.abspath(__file__))
    BusinessPath = os.path.join(currentDir, "Data", "Businesses.csv")
    IncomePath = os.path.join(currentDir, "Data", "Income.csv")
    PollingPlacesPath = os.path.join(currentDir, "Data", "PollingPlaces2019.csv")
    PopulationPath = os.path.join(currentDir, "Data", "Population.csv")
    StopsPath = os.path.join(currentDir, "Data", "Stops.txt")
    CatchmentPrimaryPath = os.path.join(currentDir, "Data", "catchments", "catchments_primary.shp")
    CatchmentSecondaryPath = os.path.join(currentDir, "Data", "catchments", "catchments_secondary.shp")
    CatchmentFuturePath = os.path.join(currentDir, "Data", "catchments", "catchments_future.shp")
    SA2DigitalBoundariesPath = os.path.join(currentDir, "Data", "SA2 Digital Boundaries","SA2_2021_AUST_GDA2020.shp")

    BusinessCSV = readCSV(BusinessPath)
    IncomeCSV = readCSV(IncomePath)
    PollingPlacesCSV = readCSV(PollingPlacesPath)
    PopulationCSV = readCSV(PopulationPath)
    StopsCSV = readCSV(StopsPath)
    CatchmentPrimary = readGeospatial(CatchmentPrimaryPath)
    CatchmentSecondary = readGeospatial(CatchmentSecondaryPath)
    CatchmentFuture = readGeospatial(CatchmentFuturePath)
    SA2DigitalBoundaries = readGeospatial(SA2DigitalBoundariesPath)

    Businesses, Income, PollingPlace, Population, Stops = cleanCSV(BusinessCSV, IncomeCSV, PollingPlacesCSV, PopulationCSV, StopsCSV)
    CPrimary, CSecondary, CFuture, SA2, Stops = cleanGeospatial(CatchmentPrimary, CatchmentSecondary, CatchmentFuture, SA2DigitalBoundaries, Stops, srid)

    credentials = "Credentials.json"
    db, conn = pgconnect(credentials)
    with open('Schemas/SA2.sql', 'r') as file:
        schema = file.read()
    query(conn, schema, df=False)