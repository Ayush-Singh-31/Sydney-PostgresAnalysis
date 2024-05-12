import os
import numpy as np
import pandas as pd
import seaborn as sns
import geopandas as gpd
import matplotlib.pyplot as plt
from geoalchemy2 import WKTElement
from shapely.geometry import MultiPolygon

def readCSV(csv) -> pd.DataFrame:
    return pd.read_csv(csv)

def exportCSV(df, path) -> None:
    df.to_csv(path, index=False)
    return None

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

    # Buiness
    Business = Business.drop_duplicates() 
    Business.rename(columns = {'industry_code':'IndustryCode'}, inplace = True)
    Business.rename(columns = {'industry_name':'IndustryName'}, inplace = True)
    Business.rename(columns = {'sa2_code':'SA2code'}, inplace = True)
    Business.rename(columns = {'sa2_name':'SA2name'}, inplace = True)
    Business.rename(columns = {'0_to_50k_businesses':'0kto50k'}, inplace = True)
    Business.rename(columns = {'50k_to_200k_businesses':'50kto200k'}, inplace = True)
    Business.rename(columns = {'200k_to_2m_businesses':'200kto2M'}, inplace = True)
    Business.rename(columns = {'2m_to_5m_businesses':'2Mto5M'}, inplace = True)
    Business.rename(columns = {'5m_to_10m_businesses':'5Mto10M'}, inplace = True)
    Business.rename(columns = {'10m_or_more_businesses':'10MOver'}, inplace = True)
    Business.rename(columns = {'total_businesses':'TotalBusinesses'}, inplace = True)

    # Income
    Income = Income.drop_duplicates()
    Income['earners'] = pd.to_numeric(Income['earners'], errors='coerce')
    Income['median_age'] = pd.to_numeric(Income['median_age'], errors='coerce')
    Income['median_income'] = pd.to_numeric(Income['median_income'], errors='coerce')
    Income['mean_income'] = pd.to_numeric(Income['mean_income'], errors='coerce')
    Income.rename(columns = {'sa2_code21':'SA2code'}, inplace = True)
    Income.rename(columns = {'sa2_name':'SA2name'}, inplace = True)
    Income.rename(columns = {'median_age':'MedianAge'}, inplace = True)
    Income.rename(columns = {'median_income':'MedianIncome'}, inplace = True)
    Income.rename(columns = {'mean_income':'MeanIncome'}, inplace = True)

    # PollingPlace
    PollingPlace = PollingPlace.drop_duplicates()
    PollingPlace.rename(columns = {'division_id':'DivisionID'}, inplace = True)
    PollingPlace.rename(columns = {'division_name':'DivisionName'}, inplace = True)
    PollingPlace.rename(columns = {'polling_place_id':'PlaceID'}, inplace = True)
    PollingPlace.rename(columns = {'polling_place_type_id':'TypeID'}, inplace = True)
    PollingPlace.rename(columns = {'polling_place_name':'PlaceName'}, inplace = True)
    PollingPlace.rename(columns = {'premises_name':'PremisesName'}, inplace = True)
    PollingPlace.rename(columns = {'premises_address_1':'Address1'}, inplace = True)
    PollingPlace.rename(columns = {'premises_address_2':'Address2'}, inplace = True)
    PollingPlace.rename(columns = {'premises_address_3':'Address3'}, inplace = True)
    PollingPlace.rename(columns = {'premises_suburb':'Suburb'}, inplace = True)
    PollingPlace.rename(columns = {'premises_state_abbreviation':'StateABV'}, inplace = True)
    PollingPlace.rename(columns = {'premises_post_code':'Postcode'}, inplace = True)
    PollingPlace.rename(columns = {'the_geom':'Geom'}, inplace = True)

    # Population
    Population = Population.drop_duplicates()
    Population.rename(columns = {'sa2_code':'SA2code'}, inplace = True)
    Population.rename(columns = {'sa2_name':'SA2name'}, inplace = True)
    Population.rename(columns = {'0-4_people':'Age0to4'}, inplace = True)
    Population.rename(columns = {'5-9_people':'Age5to9'}, inplace = True)
    Population.rename(columns = {'10-14_people':'Age10to14'}, inplace = True)
    Population.rename(columns = {'20-24_people':'Age20to24'}, inplace = True)
    Population.rename(columns = {'15-19_people':'Age15to19'}, inplace = True)
    Population.rename(columns = {'25-29_people':'Age25to29'}, inplace = True)
    Population.rename(columns = {'30-34_people':'Age30to34'}, inplace = True)
    Population.rename(columns = {'35-39_people':'Age35to39'}, inplace = True)
    Population.rename(columns = {'40-44_people':'Age40to44'}, inplace = True)
    Population.rename(columns = {'45-49_people':'Age45to49'}, inplace = True)
    Population.rename(columns = {'50-54_people':'Age50to54'}, inplace = True)
    Population.rename(columns = {'55-59_people':'Age55to59'}, inplace = True)
    Population.rename(columns = {'60-64_people':'Age60to64'}, inplace = True)
    Population.rename(columns = {'65-69_people':'Age65to69'}, inplace = True)
    Population.rename(columns = {'70-74_people':'Age70to74'}, inplace = True)
    Population.rename(columns = {'75-79_people':'Age75to79'}, inplace = True)
    Population.rename(columns = {'80-84_people':'Age80to84'}, inplace = True)
    Population.rename(columns = {'85-and-over_people':'Age85Over'}, inplace = True)
    Population.rename(columns = {'total_people':'TotalPeople'}, inplace = True)

    # Stops
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

    # SA2DigitalBoundaries
    # Refining the data
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

def create_wkt_element(geom, srid):
    if geom.geom_type == 'Polygon':
        geom = MultiPolygon([geom])
    return WKTElement(geom.wkt, srid)

if __name__ == "__main__":

    # Declareing Global Variables
    srid = 4326

    # Setting the path for the data files
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

    # Reading the data files 
    BusinessCSV = readCSV(BusinessPath)
    IncomeCSV = readCSV(IncomePath)
    PollingPlacesCSV = readCSV(PollingPlacesPath)
    PopulationCSV = readCSV(PopulationPath)
    StopsCSV = readCSV(StopsPath)
    CatchmentPrimary = readGeospatial(CatchmentPrimaryPath)
    CatchmentSecondary = readGeospatial(CatchmentSecondaryPath)
    CatchmentFuture = readGeospatial(CatchmentFuturePath)
    SA2DigitalBoundaries = readGeospatial(SA2DigitalBoundariesPath)

    # Describing the dataframes
    describeData(BusinessCSV, 'Data Description/BusinessDescription.txt')
    describeData(IncomeCSV, 'Data Description/IncomeDescription.txt')
    describeData(PollingPlacesCSV, 'Data Description/PollingPlacesDescription.txt')
    describeData(PopulationCSV, 'Data Description/PopulationDescription.txt')
    describeData(StopsCSV, 'Data Description/StopsDescription.txt')
    describeData(CatchmentPrimary, 'Data Description/CatchmentPrimaryDescription.txt')
    describeData(CatchmentSecondary, 'Data Description/CatchmentSecondaryDescription.txt')
    describeData(CatchmentFuture, 'Data Description/CatchmentFutureDescription.txt')
    describeData(SA2DigitalBoundaries, 'Data Description/SA2DigitalBoundariesDescription.txt')

    # Cleaning the data while keeping the original data-files
    Businesses, Income, PollingPlace, Population, Stops = cleanCSV(BusinessCSV, IncomeCSV, PollingPlacesCSV, PopulationCSV, StopsCSV)
    CPrimary, CSecondary, CFuture, SA2, Stops = cleanGeospatial(CatchmentPrimary, CatchmentSecondary, CatchmentFuture, SA2DigitalBoundaries, Stops, srid)

    # Exporting Clean Data
    exportCSV(Businesses, 'Data/Cleaned/Businesses.csv')
    exportCSV(Income, 'Data/Cleaned/Income.csv')
    exportCSV(PollingPlace, 'Data/Cleaned/PollingPlace.csv')
    exportCSV(Population, 'Data/Cleaned/Population.csv')
    exportCSV(Stops, 'Data/Cleaned/Stops.csv')
    exportCSV(CPrimary, 'Data/Cleaned/CatchmentPrimary.csv')
    exportCSV(CSecondary, 'Data/Cleaned/CatchmentSecondary.csv')
    exportCSV(CFuture, 'Data/Cleaned/CatchmentFuture.csv')
    exportCSV(SA2, 'Data/Cleaned/SA2DigitalBoundaries.csv')