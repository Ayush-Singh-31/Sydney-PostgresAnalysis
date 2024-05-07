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
    -- FOREIGN KEY (SA2_code) REFERENCES Income(SA2_code)
);
CREATE TABLE Businesses (
    Industry_code VARCHAR(255),
    Industry_name VARCHAR(255),
    SA2_code INTEGER PRIMARY KEY,
    SA2_name VARCHAR(255),
    0_to_50k_businesses INTEGER,
    50k_to_200k_businesses INTEGER,
    200k_to_2m_businesses INTEGER,
    2m_to_5m_businesses INTEGER,
    5m_to_10m_businesses INTEGER,
    10m_or_more_businesses INTEGER,
    Total_businesses INTEGER,
    FOREIGN KEY (SA2_code)
);
CREATE TABLE PollingPlaces (
    FID VARCHAR(255) PRIMARY KEY,
    State_ VARCHAR(255),
    Division_name VARCHAR(255),
    Division_id INTEGER,
    Polling_place_type_id INTEGER,
    Polling_place_name VARCHAR(255),
    Premises_name VARCHAR(255),
    Premises_address_1 VARCHAR(255),
    Premises_address_2 VARCHAR(255),
    Premises_address_3 VARCHAR(255),
    Premises_suburb VARCHAR(255),
    Premises_state_abbreviation VARCHAR(255),
    Premises_postal_code INTEGER,
    Latitude NUMERIC,
    Longitude NUMERIC,
    The_geom GEOMETRY(POINT,4326),
    -- FOREIGN KEY (SA2_ID) REFERENCES SA2_Regions(SA2_ID)
),

CREATE TABLE Stops (
    Stop_id INTEGER,
    Stop_code INTEGER,
    Stop_name VARCHAR(255),
    Location_type VARCHAR(255),
    Parent_station VARCHAR(255),
    Wheelchair_boarding INTEGER,
    Platform_code INTEGER,
    geom GEOMETRY(POINT,4326)
),

CREATE TABLE Primary (
    USE_ID INTEGER PRIMARY KEY,
    Catch_type VARCHAR (255),
    Use_desc VARCHAR(255),
    Add_date INTEGER,
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
    Year12 VARCHAR,
),

CREATE TABLE Secondary (
    USE_ID INTEGER PRIMARY KEY,
    Catch_type VARCHAR (255),
    Use_desc VARCHAR(255),
    Add_date INTEGER,
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
    Year12 VARCHAR,
),

CREATE TABLE Future (
    USE_ID INTEGER PRIMARY KEY,
    Catch_type VARCHAR (255),
    Use_desc VARCHAR(255),
    Add_date INTEGER,
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
    Year12 INTEGER,
)