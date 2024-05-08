import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import geopandas as gpd

from shapely.geometry import MultiPolygon
from geoalchemy2 import WKTElement

def readCSV(csv):
    data = pd.read_csv(csv)
    return data

def readGeospatial(path):
    return gpd.read_file(path)

def cleanData(Business, Income, PollingPlace, Population, Stops):

    # Drop rows with NaN values
    Business = Business.dropna()
    Income = Income.dropna()
    PollingPlace = PollingPlace.dropna()
    Population = Population.dropna()

    # Cleaning Duplicates
    Business = Business.drop_duplicates()
    Income = Income.drop_duplicates()
    PollingPlace = PollingPlace.drop_duplicates()
    Population = Population.drop_duplicates()
    Stops = Stops.drop_duplicates()

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

    return Business, Income, PollingPlace, Population, Stops


def cleanGeospatial(CatchmentPrimary, CatchmentSecondary, CatchmentFuture, SA2DigitalBoundaries):
    CatchmentPrimary_cleaned = CatchmentPrimary.dropna(subset=['ADD_DATE'])
    CatchmentSecondary_cleaned = CatchmentSecondary.dropna(subset=['ADD_DATE'])

    CatchmentPrimary_cleaned.plot()
    plt.savefig('Plots/CatchmentPrimary.png')
    CatchmentSecondary_cleaned.plot()
    plt.savefig('Plots/CatchmentSecondary.png')
    CatchmentFuture.plot()
    plt.savefig('Plots/CatchmentFuture.png')
    SA2DigitalBoundaries.plot()
    plt.savefig('Plots/SA2DigitalBoundaries.png')

    return CatchmentPrimary_cleaned, CatchmentSecondary_cleaned, CatchmentFuture, SA2DigitalBoundaries

def create_wkt_element(geom, srid):
    if geom.geom_type == 'Polygon':
        geom = MultiPolygon([geom])
    return WKTElement(geom.wkt, srid)


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

    Business, Income, PollingPlace, Population, Stops = cleanData(BusinessCSV, IncomeCSV, PollingPlacesCSV, PopulationCSV, StopsCSV)
    CatchmentPrimary, CatchmentSecondary, CatchmentFuture, SA2DigitalBoundaries = cleanGeospatial(CatchmentPrimary, CatchmentSecondary, CatchmentFuture, SA2DigitalBoundaries)
    
    StopsCSV['geom'] = gpd.points_from_xy(StopsCSV.stop_lon, StopsCSV.stop_lat)  # creating the geometry column
    StopsCSV = StopsCSV.drop(columns=['stop_lat', 'stop_lon'])  # removing the old latitude/longitude fields
    
    StopsCSV['geom'] = StopsCSV['geom'].apply(lambda x: WKTElement(x.wkt, srid=srid))
    print(StopsCSV.head())
    
    CatchmentPrimaryog = CatchmentPrimary.copy()  # creating a copy of the original for later
    CatchmentPrimary['geom'] = CatchmentPrimary['geometry'].apply(lambda x: create_wkt_element(geom=x,srid=srid))  # applying the function
    CatchmentPrimary = CatchmentPrimary.drop(columns="geometry")  # deleting the old copy
    print(CatchmentPrimary.head())
    
    CatchmentFutureog = CatchmentFuture.copy()  # creating a copy of the original for later
    CatchmentFuture['geom'] = CatchmentFuture['geometry'].apply(lambda x: create_wkt_element(geom=x,srid=srid))  # applying the function
    CatchmentFuture = CatchmentFuture.drop(columns="geometry")  # deleting the old copy
    print(CatchmentFuture.head())
            
    CatchmentSecondaryog = CatchmentSecondary.copy()  # creating a copy of the original for later
    CatchmentSecondary['geom'] = CatchmentSecondary['geometry'].apply(lambda x: create_wkt_element(geom=x,srid=srid))  # applying the function
    CatchmentSecondary = CatchmentSecondary.drop(columns="geometry")  # deleting the old copy
    print(CatchmentSecondary.head())
