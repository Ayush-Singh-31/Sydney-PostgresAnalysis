CREATE TABLE CatchmentsPrimary (
    USE_ID INTEGER PRIMARY KEY,
    CATCH_TYPE VARCHAR(255),
    USE_DESC VARCHAR(255),
    ADD_DATE DATE,
    KINDERGART VARCHAR(255),
    YEAR1 VARCHAR(255),
    YEAR2 VARCHAR(255),
    YEAR3 VARCHAR(255),
    YEAR4 VARCHAR(255),
    YEAR5 VARCHAR(255),
    YEAR6 VARCHAR(255),
    YEAR7 VARCHAR(255),
    YEAR8 VARCHAR(255),
    YEAR9 VARCHAR(255),
    YEAR10 VARCHAR(255),
    YEAR11 VARCHAR(255),
    YEAR12 VARCHAR(255),
    PRIORITYCOL VARCHAR(255),
    -- Check the above Col
    Geom VARCHAR(255)
);

CREATE TABLE CatchmentsSecondary (
    USE_ID INTEGER PRIMARY KEY,
    CATCH_TYPE VARCHAR(255),
    USE_DESC VARCHAR(255),
    ADD_DATE DATE,
    KINDERGART VARCHAR(255),
    YEAR1 VARCHAR(255),
    YEAR2 VARCHAR(255),
    YEAR3 VARCHAR(255),
    YEAR4 VARCHAR(255),
    YEAR5 VARCHAR(255),
    YEAR6 VARCHAR(255),
    YEAR7 VARCHAR(255),
    YEAR8 VARCHAR(255),
    YEAR9 VARCHAR(255),
    YEAR10 VARCHAR(255),
    YEAR11 VARCHAR(255),
    YEAR12 VARCHAR(255),
    PRIORITYCOL VARCHAR(255),
    -- Check the above Col
    Geom VARCHAR(255)
    -- Check the above Col
);

CREATE TABLE CatchmentsFuture (
    USE_ID INTEGER PRIMARY KEY,
    CATCH_TYPE VARCHAR(255),
    USE_DESC VARCHAR(255),
    ADD_DATE DATE,
    KINDERGART INT,
    YEAR1 INT,
    YEAR2 INT,
    YEAR3 INT,
    YEAR4 INT,
    YEAR5 INT,
    YEAR6 INT,
    YEAR7 INT,
    YEAR8 INT,
    YEAR9 INT,
    YEAR10 INT,
    YEAR11 INT,
    YEAR12 INT,
    Geom GEOMETRY(POINT,4326)
    -- Check the above Col
);

CREATE TABLE SA2 (
    "SA2Code" INTEGER PRIMARY KEY,
    "SA2Name" VARCHAR(255),
    "CHGFlag" INTEGER,
    "CHGLabel" VARCHAR(255),
    "SA3Code" INTEGER,
    "SA3Name" VARCHAR(255),
    "SA4Code" INTEGER,
    "SA4Name" VARCHAR(255),
    "GCCCode" VARCHAR(255),
    "GCCName" VARCHAR(255),
    "STECode" INTEGER,
    "STEName" VARCHAR(255),
    "AUSCode" VARCHAR(255),
    "AUSName" VARCHAR(255),
    "AREA" FLOAT,
    "Geometry" GEOMETRY(GEOMETRY, 4326)
);

CREATE TABLE Income (
    SA2code INTEGER PRIMARY KEY,
    SA2name VARCHAR(255),
    earners FLOAT,
    MedianAge FLOAT,
    MedianIncome FLOAT,
    MeanIncome FLOAT
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
    IndustryCode VARCHAR(255),
    IndustryName VARCHAR(255),
    SA2code INT,
    SA2name VARCHAR(255),
    "0kto50k" INT,
    "50kto200k" INT,
    "200kto2M" INT,
    "2Mto5M" INT,
    "5Mto10M" INT,
    "10MOver" INT,
    TotalBusinesses INT,
    FOREIGN KEY (SA2code) REFERENCES SA2(SA2code)
);

-- Change State Col name to StateABV
CREATE TABLE PollingPlaces (
    FOREIGN KEY (SA2code) REFERENCES SA2(SA2code),
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

-- Drop Geometry in cleaning
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