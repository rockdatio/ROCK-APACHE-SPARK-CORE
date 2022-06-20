from pyspark.sql import SparkSession

if __name__ == '__main__':
    path = "/tmp/data/workspace/Data - Laboratorio/ExportCSV.csv"
    spark = SparkSession.builder.appName('SelectFilter').getOrCreate()
    sc = spark.sparkContext
    # Reading main data
    df_with_nulls = spark.read.format("csv").option("header", "true").load(path)
    # Dropping nulls
    df_cleaned = df_with_nulls.na.drop()

    new_df = df_cleaned.limit(8)

    dept = [("Logistics", 10), \
            ("Inventory", 20), \
            ("Domestic operations", 30), \
            ("International Operations", 40), \
            ("Jewelry", 50), \
            ("Cosmetics", 60), \
            ]
    deptColumns = ["Department name", "Department ID"]
    deptDF = spark.createDataFrame(data=dept, schema=deptColumns)
    deptDF.show()

    print("INNER JOIN")
    print("==========")
    new_df.join(deptDF, new_df["Retail Department"] == deptDF["Department name"], "inner") \
        .show()

    print("OUTER JOIN")
    print("==========")
    new_df.join(deptDF, new_df["Retail Department"] == deptDF["Department name"], "outer") \
        .show()

    print("LEFT JOIN")
    print("==========")
    new_df.join(deptDF, new_df["Retail Department"] == deptDF["Department name"], "left") \
        .show()

    print("RIGHT JOIN")
    print("==========")
    new_df.join(deptDF, new_df["Retail Department"] == deptDF["Department name"], "right") \
        .show()

    spark.stop()
