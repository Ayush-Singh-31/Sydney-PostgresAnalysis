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
sql = """
DROP TABLE IF EXISTS helper;
CREATE TABLE helper AS
SELECT s.SA2_CODE21, s.SA2_NAME21, SUM(p."0-4_people" + p."5-9_people" + p."10-14_people" + p."15-19_people") AS under_19
FROM SA2 s, POPULATION p
WHERE s.SA2_CODE21 = p.sa2_code
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
-- yeh hai school wala
ql = """
    DROP TABLE IF EXISTS helper;
    CREATE TABLE helper AS
    SELECT s.SA2_CODE21, s.SA2_NAME21, SUM(p."0-4_people" + p."5-9_people" + p."10-14_people" + p."15-19_people") AS under_19
    FROM SA2 s, POPULATION p
    WHERE s.SA2_CODE21 = p.sa2_code
    GROUP BY s.SA2_CODE21, s.SA2_NAME21;
    """
    query(conn, sql)
    print(pd.read_sql_query("SELECT * from helper;", conn))
    
    
    sql ="""
    DROP TABLE IF EXISTS school_table;
    CREATE TABLE school_table AS
    SELECT s.SA2_CODE21, s.SA2_NAME21, CAST(COUNT(*) AS FLOAT) *1000/h.under_19 AS avg
    FROM SA2 s
    JOIN School sc ON ST_INTERSECTS(sc."Geometry", s.geom)
    JOIN helper h ON (h.SA2_CODE21 = s.SA2_CODE21)
    WHERE h.under_19 > 0
    GROUP BY s.SA2_CODE21, s.SA2_NAME21, under_19
    
    """
    
    query(conn, sql)
    print(pd.read_sql_query("SELECT * from school_table;", conn))
    
    
    sql ="""
    ALTER TABLE school_table ADD IF NOT EXISTS zschool FLOAT;
    UPDATE school_table SET zschool = (avg - (SELECT AVG(avg) FROM school_table)) / (SELECT STDDEV(avg) FROM school_table);
    SELECT * FROM school_table order by zschool desc;
    
    """
    
    query(conn, sql)
    print(pd.read_sql_query("SELECT * from school_table;", conn))
