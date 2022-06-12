from pyspark.sql import SparkSession

if __name__ == '__main__':
    data = "/tmp/data/workspace/Data - Laboratorio/ExportCSV.csv"
    spark = SparkSession.builder.appName('Examples').getOrCreate()
    df_with_nulls = spark.read.format("csv").option("header", "true").load(data)
    df_final = df_with_nulls.na.drop()

    df_final.write.mode('overwrite').parquet('/tmp/data/hadoop/landing/wrongRecordsTest')
    # why is importante to rename column ?

    # FILTER DATA TO DROP
    df_to_drop = df_final \
        .withColumnRenamed("Purchase (USD)", "Purchase") \
        .withColumnRenamed("First Name", "First_Name") \
        .withColumnRenamed("Last Name", "Last_Name") \
        .withColumnRenamed("Retail Department", "Retail_Department")

    # df_to_drop.write.mode('overwrite').parquet('/tmp/data/hadoop/landing/wrongRecordsTest')

    spark.stop()
