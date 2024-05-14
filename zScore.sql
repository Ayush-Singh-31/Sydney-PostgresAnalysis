-- This creates zbusiness
schema = """
CREATE TABLE buss_table AS
SELECT p.sa2_code, p.sa2_name,  (CAST(b.total_businesses AS FLOAT) / p.total_people * 1000 - AVG(CAST(b.total_businesses AS FLOAT) / p.total_people * 1000) OVER ()) / STDDEV_POP(CAST(b.total_businesses AS FLOAT) / p.total_people * 1000) OVER () AS zbusiness
FROM Business b JOIN Population p 
ON p.sa2_code = b.sa2_code 
WHERE industry_name = 'Electricity, Gas, Water and Waste Services'
"""

-- Now creating zstops

schema = """
CREATE TABLE stops_table AS 
SELECT s.SA2_CODE21, s.SA2_NAME21, 
    (COUNT(st.stop_id) - AVG(COUNT(st.stop_id)) OVER ()) / STDDEV_POP(COUNT(st.stop_id)) OVER () AS zstops
FROM SA2 s 
JOIN (
    SELECT stop_id, geom
    FROM Stops
) st ON ST_Contains(s.geom, st.geom)
GROUP BY s.SA2_CODE21, s.SA2_NAME21;
"""

-- Now creating zpoll

schema = """
    DROP TABLE IF EXISTS poll_table;
    CREATE TABLE poll_table AS
    SELECT s.SA2_CODE21, s.SA2_NAME21, (COUNT(p.division_name) - AVG(COUNT(p.division_name)) OVER ()) / STDDEV_POP(COUNT(p.division_name)) OVER () AS zpoll
    FROM SA2 s JOIN PollingPlace p ON ST_Contains(s.geom, p.geom)
    GROUP BY s.SA2_CODE21, s.SA2_NAME21;
    """

-- zschool
schema = """
CREATE TABLE school_table AS
SELECT s.SA2_CODE21, s.SA2_NAME21, (COUNT(sc.school_id) / SUM(p.0-4_people,p.5-9_people,p.10-14_people,p.15-19_people) * 1000 - AVG(COUNT(sc.school_id) / SUM(p.0-4_people,p.5-9_people,p.10-14_people,p.15-19_people) * 1000) OVER ()) / STDDEV_POP(COUNT(sc.school_id) / SUM(p.0-4_people,p.5-9_people,p.10-14_people,p.15-19_people) * 1000) OVER () AS zschool
FROM Population p JOIN SA2 s ON (p.sa2_code = s.SA2_CODE21)
JOIN School sc ON ST_Contains(sc.Geometry, s.geom)
"""

-- trees

schema = """
    DROP TABLE IF EXISTS tree_table;
    CREATE TABLE tree_table AS
    SELECT s.SA2_CODE21, s.SA2_NAME21, (COUNT(t."ObjectId") - AVG(COUNT(t."ObjectId")) OVER ()) / STDDEV_POP(COUNT(t."ObjectId")) OVER () AS ztrees
    FROM SA2 s JOIN trees t ON ST_Contains(s.geom, t."Geometry")
    GROUP BY s.SA2_CODE21, s.SA2_NAME21;
"""

--parking

schema = """
    DROP TABLE IF EXISTS park_table;
    CREATE TABLE park_table AS
    SELECT s.SA2_CODE21, s.SA2_NAME21, (COUNT(p."OBJECTID") - AVG(COUNT(p."OBJECTID")) OVER ()) / STDDEV_POP(COUNT(p."OBJECTID")) OVER () AS zpark
    FROM SA2 s JOIN parking p ON ST_Contains(s.geom, p."Geometry")
    GROUP BY s.SA2_CODE21, s.SA2_NAME21;
"""

-- stairs
sql = """
    DROP TABLE IF EXISTS stair_table;
    CREATE TABLE stair_table AS
    SELECT s.SA2_CODE21, s.SA2_NAME21, (COUNT(st."OBJECTID") - AVG(COUNT(st."OBJECTID")) OVER ()) / STDDEV_POP(COUNT(st."OBJECTID")) OVER () AS zstairs
    FROM SA2 s JOIN stairs st ON ST_Contains(s.geom, st."Geometry")
    GROUP BY s.SA2_CODE21, s.SA2_NAME21;
"""

