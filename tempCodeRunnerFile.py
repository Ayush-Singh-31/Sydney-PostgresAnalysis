def importStairs(currentDir, conn) -> None:
    stairsPath = os.path.join(currentDir, "Data", "stairs.geojson")
    Stairs = gpd.read_file(stairsPath)
    Stairs = Stairs.drop(columns=['ID', 'No_Steps', 'TGSI', 'StairNosingConstrastStrip','ClosestAlternateRoutes', 'Photo'])
    Stairs['geometry'] = Stairs['geometry'].apply(lambda x: create_wkt_element(geom=x, srid=4326))
    print(Stairs.head())
    schema = """
    DROP TABLE IF EXISTS stairs;
    CREATE TABLE stairs (
        OBJECTID INTEGER,
        Name VARCHAR(255),
        Address VARCHAR(128),
        Suburb VARCHAR(64),
        HandRails VARCHAR(32),
        geometry GEOMETRY(POINT, 4326)
    );
    """
    try:
        conn.execute(text(schema))
        print("Table created successfully.")
    except Exception as e:
        print("Error executing SQL statement:", e)
    try:
        Stairs.to_sql("stairs", conn, if_exists='append', index=False, dtype={'geometry': Geometry('POINT', 4326)})
        print("Data inserted successfully.")
    except Exception as e: