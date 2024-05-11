CREATE TABLE CatchmentsPrimary (
    USE_ID INTEGER PRIMARY KEY,
    Catch_type VARCHAR(255),
    Use_desc VARCHAR(255),
    Add_date DATE,
    Kindergarten VARCHAR,
    Year1 VARCHAR,
    Year2 VARCHAR,
    Year3 VARCHAR,
    Year4 VARCHAR,
    Year5 VARCHAR,
    Year6 VARCHAR,
    Year7 VARCHAR,
    Year8 VARCHAR,
    Year9 VARCHAR,
    Year10 VARCHAR,
    Year11 VARCHAR,
    Year12 VARCHAR
);

CREATE TABLE CatchmentsSecondary (
    USE_ID INTEGER PRIMARY KEY,
    Catch_type VARCHAR(255),
    Use_desc VARCHAR(255),
    Add_date DATE,
    Kindergarten VARCHAR,
    Year1 VARCHAR,
    Year2 VARCHAR,
    Year3 VARCHAR,
    Year4 VARCHAR,
    Year5 VARCHAR,
    Year6 VARCHAR,
    Year7 VARCHAR,
    Year8 VARCHAR,
    Year9 VARCHAR,
    Year10 VARCHAR,
    Year11 VARCHAR,
    Year12 VARCHAR
);

CREATE TABLE CatchmentsFuture (
    USE_ID INTEGER PRIMARY KEY,
    Catch_type VARCHAR(255),
    Use_desc VARCHAR(255),
    Add_date DATE,
    Kindergarten VARCHAR,
    Year1 INTEGER,
    Year2 INTEGER,
    Year3 INTEGER,
    Year4 INTEGER,
    Year5 INTEGER,
    Year6 INTEGER,
    Year7 INTEGER,
    Year8 INTEGER,
    Year9 INTEGER,
    Year10 INTEGER,
    Year11 INTEGER,
    Year12 INTEGER
);

CREATE TABLE SA2 (
    SA2Code INTEGER PRIMARY KEY,
    SA2Name VARCHAR(255),
    CHGFalg INTEGER,
    CHGLable VARCHAR(255),
    SA3Code INTEGER,
    SA3Name VARCHAR(255),
    SA4Code INTEGER,
    SA4Name VARCHAR(255),
    GCCCode VARCHAR(255),
    GCCName VARCHAR(255),
    STECode INTEGER,
    STEName VARCHAR(255),
    AUSCode VARCHAR(255),
    AUSName VARCHAR(255),
    AREA FLOAT
);


CREATE TABLE Income (
    SA2code INTEGER PRIMARY KEY,
    SA2name VARCHAR(255),
    Earners INTEGER,
    MedianAge INTEGER,
    MedianIncome INTEGER,
    MeanIncome INTEGER
);

CREATE TABLE Population (
    SA2code INTEGER PRIMARY KEY,
    SA2name VARCHAR(255),
    Age0to4 INTEGER,
    Age5to9 INTEGER,
    Age10to14 INTEGER,
    Age15to19 INTEGER,
    Age20to24 INTEGER,
    Age25to29 INTEGER,
    Age30to34 INTEGER,
    Age35to39 INTEGER,
    Age40to44 INTEGER,
    Age45to49 INTEGER,
    Age50to54 INTEGER,
    Age55to59 INTEGER,
    Age60to64 INTEGER,
    Age65to69 INTEGER,
    Age70to74 INTEGER,
    Age75to79 INTEGER,
    Age80to84 INTEGER,
    Age85Over INTEGER,
    TotalPeople INTEGER
);

CREATE TABLE Businesses (
	FOREIGN KEY (SA2code) REFERENCES SA2(SA2code),
    IndustryCode VARCHAR(255),
    IndustryName VARCHAR(255),
    SA2code INTEGER,
    SA2name VARCHAR(255),
    Earning0kto50k INTEGER,
    Earning50kto200k INTEGER,
    Earning200kto2M INTEGER,
    Earning2Mto5M INTEGER,
    Earning5Mto10M INTEGER,
    Earning10MOver INTEGER,
    TotalBusinesses INTEGER
);

CREATE TABLE PollingPlaces (
    FID VARCHAR(255) PRIMARY KEY,
    DivisionName VARCHAR(255),
    DivisionID INTEGER,
    PlaceID INTEGER,
    TypeID INTEGER,
    PlaceName VARCHAR(255),
    PremisesName VARCHAR(255),
    Address1 VARCHAR(255),
    Address2 VARCHAR(255),
    Address3 VARCHAR(255),
    Suburb VARCHAR(255),
    StateABV VARCHAR(255),
    Postcode INTEGER,
    Latitude NUMERIC,
    Longitude NUMERIC,
    Geom GEOMETRY(POINT,4326)
);

CREATE TABLE Stops (
    StopID INTEGER,
    StopCode INTEGER,
    StopName VARCHAR(255),
    LocationType VARCHAR(255),
    ParentStation VARCHAR(255),
    WheelchairBoarding INTEGER,
    PlatformCode INTEGER,
    geom GEOMETRY(POINT,4326)
);