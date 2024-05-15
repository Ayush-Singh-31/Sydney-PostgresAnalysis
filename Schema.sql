
CREATE TABLE SA2 (
    "sa2_code21" INTEGER,
    "sa2_name21" VARCHAR(255),
    "AREASQKM21" FLOAT,
    "geom" GEOMETRY(MULTIPOLYGON, 4326)
);

CREATE TABLE School (
    "USE_ID" INTEGER,
    "CATCH_TYPE" VARCHAR(255),
    "USE_DESC" VARCHAR(255),
    "Geometry" GEOMETRY(MULTIPOLYGON, 4326)
);

CREATE TABLE Business (
    "industry_name" VARCHAR(255),
    "sa2_code" INTEGER,
    "sa2_name" VARCHAR(255),
    "0_to_50k_businesses" INTEGER,
    "50k_to_200k_businesses" INTEGER,
    "200k_to_2m_businesses" INTEGER,
    "2m_to_5m_businesses" INTEGER,
    "5m_to_10m_businesses" INTEGER,
    "10m_or_more_businesses" INTEGER,
    "total_businesses" INTEGER
);

CREATE TABLE Income (
    "sa2_code21" INTEGER,
    "sa2_name" VARCHAR(255),
    "earners" INTEGER,
    "median_age" INTEGER,
    "median_income" INTEGER,
    "mean_income" INTEGER
);

CREATE TABLE PollingPlace (
    "division_name" VARCHAR(255),
    "polling_place_name" VARCHAR(255),
    geom GEOMETRY(POINT, 4326)
);

CREATE TABLE Population (
    "sa2_code" INTEGER,
    "sa2_name" VARCHAR(255),
    "0-4_people" INTEGER,
    "5-9_people" INTEGER,
    "10-14_people" INTEGER,
    "15-19_people" INTEGER,
    "20-24_people" INTEGER,
    "25-29_people" INTEGER,
    "30-34_people" INTEGER,
    "35-39_people" INTEGER,
    "40-44_people" INTEGER,
    "45-49_people" INTEGER,
    "50-54_people" INTEGER,
    "55-59_people" INTEGER,
    "60-64_people" INTEGER,
    "65-69_people" INTEGER,
    "70-74_people" INTEGER,
    "75-79_people" INTEGER,
    "80-84_people" INTEGER,
    "85-and-over_people" INTEGER,
    "total_people" INTEGER
);

CREATE TABLE Stops (
    "stop_id" VARCHAR(255),
    "stop_code" INTEGER,
    "stop_name" VARCHAR(255),
    "geom" GEOMETRY(POINT, 4326)
);

CREATE TABLE trees (
    "ObjectId" INTEGER,
    "Geometry" GEOMETRY(POINT, 4326)
);

CREATE TABLE Parking (
    "OBJECTID" INTEGER,
    "SiteID" INTEGER,
    "Suburb" VARCHAR(32),
    "NumberParkingSpaces" INTEGER,
    "Geometry" GEOMETRY(POINT, 4326)
);

CREATE TABLE stairs (
    "OBJECTID" INTEGER,
    "Name" VARCHAR(255),
    "Address" VARCHAR(128),
    "Suburb" VARCHAR(64),
    "HandRails" VARCHAR(32),
    "Geometry" GEOMETRY(POINT, 4326)
);