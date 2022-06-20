from pyspark.sql import SparkSession

if __name__ == '__main__':
    path = "/tmp/data/workspace/Data - Laboratorio/ExportCSV.csv"
    spark = SparkSession.builder.appName('SelectFilter').getOrCreate()
    sc = spark.sparkContext
    # Reading main data
    df_with_nulls = spark.read.format("csv").option("header", "true").load(path)
    # Dropping nulls
    df_cleaned = df_with_nulls.na.drop()

    df_cleaned.createOrReplaceTempView("shopping")
    sqlDF = spark.sql("SELECT * FROM shopping")
    # spark.catalog.dropTempView("shopping")
    sqlDF.show()
    spark.stop()
