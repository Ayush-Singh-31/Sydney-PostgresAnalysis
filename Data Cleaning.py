import os
import pandas as pd

def readCSV(csv):
    data = pd.read_csv(csv)
    return data

if __name__ == "__main__":
    currentDir = os.path.dirname(os.path.abspath(__file__))

    BusinessPath = os.path.join(currentDir, "Data", "Businesses.csv")
    IncomePath = os.path.join(currentDir, "Data", "Income.csv")
    PollingPlacesPath = os.path.join(currentDir, "Data", "PollingPlaces2019.csv")
    PopulationPath = os.path.join(currentDir, "Data", "Population.csv")

    BusinessCSV = readCSV(BusinessPath)
    IncomeCSV = readCSV(IncomePath)
    PollingPlacesCSV = readCSV(PollingPlacesPath)
    PopulationCSV = readCSV(PopulationPath)

    print(BusinessCSV.head())
    print(IncomeCSV.head())
    print(PollingPlacesCSV.head())
    print(PopulationCSV.head())
