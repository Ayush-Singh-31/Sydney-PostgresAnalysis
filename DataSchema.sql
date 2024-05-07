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

