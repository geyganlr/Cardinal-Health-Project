import psycopg2

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host="db1.cl5uuoi3dsos.us-east-2.rds.amazonaws.com",
    database="cads",
    user="postgres",
    password="Cads=post1"
)

def importData (textFile, tableName):
    with open(textFile, "r") as f:
        # Skip the first line of the file
        next(f)

        # Create a cursor object
        cur = conn.cursor()

        try:
            cur.execute("TRUNCATE " + tableName)
        except:
            cur.execute("rollback")
            cur.execute("TRUNCATE " + tableName)

        # Use the COPY function to load data from the text file
        cur.copy_from(f, tableName, sep="|", null=" ")

        # Commit the changes to the database
        conn.commit()

        # Close the cursor and connection
        cur.close()

        print("Table " + tableName + " imported")

importData("plan information  20230228.txt", "plan_information")
importData("senior savings model file 20230228.txt", "senior_savings_model")
importData("basic drugs formulary file  20230228.txt", "basic_drug_formulary")
importData("excluded drugs formulary file  20230228.txt", "excluded_drugs_formulary")
importData("pharmacy networks file  20230228 part 1.txt", "pharmacy_networks")
importData("partial gap coverage file  20230228.txt", "partial_gap_coverage")
importData("Indication Based Coverage Formulary File  20230228.txt", "indication_based_coverage_formulary")
importData("geographic locator file 20230228.txt", "geographic_locator")
importData("beneficiary cost file  20230228.txt", "beneficiary_cost")
importData("pharmacy networks file  20230228 part 2.txt", "pharmacy_networks")
importData("pharmacy networks file  20230228 part 3.txt", "pharmacy_networks")
importData("pharmacy networks file  20230228 part 4.txt", "pharmacy_networks")
importData("pharmacy networks file  20230228 part 5.txt", "pharmacy_networks")
importData("pharmacy networks file  20230228 part 6.txt", "pharmacy_networks")


# close connection
conn.close()