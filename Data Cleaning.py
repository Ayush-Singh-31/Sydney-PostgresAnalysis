import pandas as pd

def readCSV(csv):
    data = pd.read_csv(csv)
    return data

if __name__ == "__main__":
    BusinessCSV = readCSV("Data/Businesses.csv")
    IncomeCSV = readCSV("Data/Income.csv")
    PollingPlacesCSV = readCSV("Data/PollingPlaces.csv")
    PopulationCSV = readCSV("Data/Population.csv")
    print(BusinessCSV.head())
    print(IncomeCSV.head())
    print(PollingPlacesCSV.head())
    print(PopulationCSV.head())
