def importCPrimary(currentDir, conn) -> None:
    CatchmentPrimaryPath = os.path.join(currentDir, "Data", "catchments", "catchments_primary.shp")
    CatchmentPrimary = gpd.read_file(CatchmentPrimaryPath)
    CatchmentPrimary['Geometry'] = CatchmentPrimary['geometry'].apply(lambda x: create_wkt_element(geom=x,srid=4326))
    CatchmentPrimary = CatchmentPrimary.drop(columns="geometry")
    CatchmentPrimary.drop_duplicates()

    CatchmentPrimary['PRIORITY'] = pd.to_numeric(CatchmentPrimary['PRIORITY'], errors='coerce').fillna(0).astype(int)

    
    print(CatchmentPrimary.head())
    print(CatchmentPrimary[["PRIORITY"]])
    schema = """
    DROP TABLE IF EXISTS CatchmentPrimary;
    CREATE TABLE CatchmentPrimary (
        "USE_ID" INTEGER PRIMARY KEY,
        "CATCH_TYPE" VARCHAR(255),
        "USE_DESC" VARCHAR(255),
        "ADD_DATE" DATE,
        "KINDERGART" BOOLEAN,
        "YEAR1" BOOLEAN,
        "YEAR2" BOOLEAN,
        "YEAR3" BOOLEAN,
        "YEAR4" BOOLEAN,
        "YEAR5" BOOLEAN,
        "YEAR6" BOOLEAN,
        "YEAR7" BOOLEAN,
        "YEAR8" BOOLEAN,
        "YEAR9" BOOLEAN,
        "YEAR10" BOOLEAN,
        "YEAR11" BOOLEAN,
        "YEAR12" BOOLEAN,
        "PRIORITY" INTEGER,
        "Geometry" GEOMETRY(MULTIPOLYGON, 4326)
    );
    """
    try:
        conn.execute(text(schema))
        print("Table created successfully.")
    except Exception as e:
        print("Error executing SQL statement:", e)
    try:
        CatchmentPrimary.to_sql("catchmentprimary", conn, if_exists='append', index=False, dtype={'Geometry': Geometry('MULTIPOLYGON', 4326)})
        print("Data inserted successfully.")
    except Exception as e:
        print("Error inserting data:", e)
    print(query(conn, "select * from catchmentprimary"))