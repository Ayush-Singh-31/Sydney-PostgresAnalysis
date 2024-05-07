CREATE TABLE Income (
    ID INT PRIMARY KEY,
    SA2_Region_ID INT,
    Year INT,
    IncomeGroup VARCHAR(255),
    AverageIncome DECIMAL(10, 2),
    FOREIGN KEY (SA2_Region_ID) REFERENCES SA2_Regions(ID),
    CONSTRAINT CHK_Year CHECK (Year > 1900 AND Year <= EXTRACT(YEAR FROM CURRENT_DATE))
);
CREATE TABLE Population (
    SA2_ID VARCHAR(255),
    AgeRange VARCHAR(50),
    PopulationCount INT,
    FOREIGN KEY (SA2_ID) REFERENCES SA2_Regions(SA2_ID)
);
CREATE TABLE Businesses (
    SA2_ID VARCHAR(255),
    IndustryType VARCHAR(255),
    TurnoverRange VARCHAR(255),
    NumberOfBusinesses INT,
    FOREIGN KEY (SA2_ID) REFERENCES SA2_Regions(SA2_ID)
);
CREATE TABLE PollingPlaces (
    SA2_ID VARCHAR(255),
    PollingPlaceID INT PRIMARY KEY,
    LocationName VARCHAR(255),
    Address VARCHAR(255),
    Longitude DECIMAL(9,6),
    Latitude DECIMAL(9,6),
    FOREIGN KEY (SA2_ID) REFERENCES SA2_Regions(SA2_ID)
);
CREATE TABLE PublicTransportStops (
    Stop_ID VARCHAR(255) PRIMARY KEY,
    Stop_Name VARCHAR(255),
    Stop_Lat DECIMAL(9,6),
    Stop_Lon DECIMAL(9,6),
    Stop_Code VARCHAR(50),
    Stop_Desc VARCHAR(255),
    Zone_ID VARCHAR(50),
    Stop_URL VARCHAR(255),
    Location_Type INT,
    Parent_Station VARCHAR(255),
    SA2_ID VARCHAR(255),
    FOREIGN KEY (SA2_ID) REFERENCES SA2_Regions(SA2_ID)
);
