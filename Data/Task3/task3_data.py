def importTrees(currentDir, conn) -> None:
    treesPath = os.path.join(currentDir, "Data", "trees.csv")
    Trees = pd.read_csv(treesPath)
    Trees.drop_duplicates()
    Trees.drop(columns=['OID_', 'asset_id', 'SpeciesName', 
                                  'CommonName', 'TreeHeight', 'TreeCanopyEW', 
                                  'TreeCanopyNS', 'Tree_Status', 'TreeType'], inplace=True)
    Trees['geom'] = gpd.points_from_xy(Trees.Longitude, Trees.Latitude) 
    Trees['Geometry'] = Trees['geom'].apply(lambda x: WKTElement(x.wkt, srid=4326))
    Trees = Trees.drop(columns=['Latitude', 'Longitude','geom'])
    schema = """
    DROP TABLE IF EXISTS trees;
    CREATE TABLE trees (
        "ObjectId" INTEGER,
        "Geometry" GEOMETRY(POINT, 4326)
    );
    """

    try:
        conn.execute(text(schema))
        print("Table created successfully.")
    except Exception as e:
        print("Error executing SQL statement:", e)
    try:
        Trees.to_sql("trees", conn, if_exists='append', index=False, dtype={'Geometry': Geometry('POINT', 4326)})
        print("Data inserted successfully.")
    except Exception as e:
        print("Error inserting data:", e)
    print(query(conn, "select * from trees"))

def importParking(currentDir, conn) -> None:
    parkingPath = os.path.join(currentDir, "Data", "Mobility_parking.geojson")
    Parking = gpd.read_file(parkingPath)
    Parking['geometry'] = Parking['geometry'].apply(lambda x: create_wkt_element(geom=x, srid=4326))
    print(Parking.columns)
    Parking.drop(['Address','Street','Location','SideOfStreet','ParkingSpaceWidth','ParkingSpaceLength','ParkingSpaceAngle','SignText','URL','AuditDate'], axis=1, inplace=True)
    schema = """
    DROP TABLE IF EXISTS Parking;
    CREATE TABLE Parking (
        OBJECTID INTEGER,
        SiteID INTEGER,
        Suburb VARCHAR(32),
        NumberParkingSpaces INTEGER,
        geometry GEOMETRY(POINT, 4326)
    );
    """
    try:
        conn.execute(text(schema))
        print("Table created successfully.")
    except Exception as e:
        print("Error executing SQL statement:", e)
    try:
        Parking.to_sql("Parking", conn, if_exists='append', index=False, dtype={'geometry': Geometry('POINT', 4326)})
        print("Data inserted successfully.")
    except Exception as e:
        print("Error inserting data:", e)
    print(query(conn, "select * from Parking"))

def importStairs(currentDir, conn) -> None:
    stairsPath = os.path.join(currentDir, "Data", "stairs.geojson")
    Stairs = gpd.read_file(stairsPath)
    Stairs = Stairs.drop(columns=['ID', 'No_Steps', 'TGSI', 'StairNosingConstrastStrip','ClosestAlternateRoutes', 'Photo'])
    Stairs['geometry'] = Stairs['geometry'].apply(lambda x: create_wkt_element(geom=x, srid=4326))
    print(Stairs.head())
    schema = """
    DROP TABLE IF EXISTS Stairs;
    CREATE TABLE Stairs (
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
        print("Error executing SQL Statement:", e)
    try:
        Stairs.to_sql("Stairs", conn, if_exists='append', index=False, dtype={'geometry': Geometry('POINT', 4326)})
        print("Data inserted successfully.")
    except Exception as e:
        print("Error inserting data:", e)
    print(query(conn, "select * from Stairs"))

if __name__ == "__main__":
    credentials = "Credentials.json"
    currentDir = os.path.dirname(os.path.abspath(__file__))
    db, conn = pgconnect(credentials)

    importTrees(currentDir, conn)
    importParking(currentDir, conn)
    importStairs(currentDir, conn)