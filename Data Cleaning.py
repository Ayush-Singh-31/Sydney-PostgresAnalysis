import os
import pandas as pd

import matplotlib.pyplot as plt
import seaborn as sns

def readCSV(csv):
    data = pd.read_csv(csv)
    return data

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
    

if __name__ == "__main__":
    currentDir = os.path.dirname(os.path.abspath(__file__))

    BusinessPath = os.path.join(currentDir, "Data", "Businesses.csv")
    IncomePath = os.path.join(currentDir, "Data", "Income.csv")
    PollingPlacesPath = os.path.join(currentDir, "Data", "PollingPlaces2019.csv")
    PopulationPath = os.path.join(currentDir, "Data", "Population.csv")
    StopsPath = os.path.join(currentDir, "Data", "Stops.txt")

    BusinessCSV = readCSV(BusinessPath)
    IncomeCSV = readCSV(IncomePath)
    PollingPlacesCSV = readCSV(PollingPlacesPath)
    PopulationCSV = readCSV(PopulationPath)
    StopsCSV = readCSV(StopsPath)

    Business, Income, PollingPlace, Population, Stops = cleanData(BusinessCSV, IncomeCSV, PollingPlacesCSV, PopulationCSV, StopsCSV)
