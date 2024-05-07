CREATE TABLE Income (
    
    SA2_code INTEGER PRIMARY KEY,
    SA2_Name VARCHAR(255),
    Earners INTEGER,
    Median_age INTEGER,
    Median_income INTEGER,
    Mean_income INTEGER,
);
CREATE TABLE Population (
    SA2_code INTEGER PRIMARY KEY,
    SA2_Name VARCHAR(255),
    0_4_People INTEGER,
    5_9_People INTEGER,
    10_14_People INTEGER,
    15_19_People INTEGER,
    20_24_People INTEGER,
    25_29_People INTEGER,
    30_34_People INTEGER,
    35_39_People INTEGER,
    40_44_People INTEGER,
    45_49_People INTEGER,
    50_54_People INTEGER,
    55_59_People INTEGER,
    60_64_People INTEGER,
    65_69_People INTEGER,
    70_74_People INTEGER,
    75_79_People INTEGER,
    80_84_People INTEGER,
    85_Over INTEGER,
    Total_people INTEGER,
    FOREIGN KEY (SA2_code) REFERENCES Income(SA2_code)
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
