import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, when

if __name__ == '__main__':
    data = "/tmp/data/workspace/Data - Laboratorio/ExportCSV.csv"
    spark = SparkSession.builder.appName('Examples').getOrCreate()
    df_with_nulls = spark.read.format("csv").option("header", "true").load(data)
    print(df_with_nulls.count())  # 8972
    for column in df_with_nulls.columns:
        df_with_nulls.select(
            count(
                when(isnan(column) | col(column).isNull(), column)
            ).alias(column)).show()
    print("=============================================")
    # for column in df_with_nulls.columns:
    #     if column != "ID":
    #         df_with_nulls.select(column).distinct().show()
    print("=============================================")

    # FILTER DATA TO DROP
    df_to_drop = df_with_nulls.filter(
        (col("First Name").isNull() | isnan("First Name")) &
        (col("Last Name").isNull() | isnan("Last Name")) &
        (col("State").isNull() | isnan("State")) &
        (col("Purchase (USD)").isNull() | isnan("Purchase (USD)")) &
        (col("Date").isNull() | isnan("Date")) &
        (col("Retail Department").isNull() | isnan("Retail Department")) &
        (col("SSN").isNull()) | isnan("SSN")) \
        .withColumnRenamed("Purchase (USD)", "Purchase") \
        .withColumnRenamed("First Name", "First_Name") \
        .withColumnRenamed("Last Name", "Last_Name") \
        .withColumnRenamed("Retail Department", "Retail_Department")
    df_to_drop.show()
    df_to_drop.printSchema()
    print("=============================================")
    print(df_to_drop.count())  # 269
    print("=============================================")

    df_to_drop.write.mode('overwrite').parquet('/tmp/data/hadoop/landing/wrongRecordsTransactions')
    print("=============================================")

    df_with_nulls.select([count(when(isnan(column) | col(column).isNull(), column)).alias(column) for column in
                          df_with_nulls.columns]).show()

    df_cleaned = df_with_nulls.filter(
        (col("First Name").isNotNull()) |
        (col("Last Name").isNotNull()) |
        (col("State").isNotNull()) |
        (col("Purchase (USD)").isNotNull()) |
        (col("Date").isNotNull()) |
        (col("Retail Department").isNotNull()) |
        (col("SSN").isNotNull()))

    print("=============================================")
    print(df_cleaned.count())  # 8972 - 269
    # 8703
    print("=============================================")
    # REGLAS DE INPUTACIÓN

    df_final = df_cleaned.na.drop()

    df_final.select([count(when(isnan(column) | col(column).isNull(), column)).alias(column) for column in
                     df_final.columns]).show()
    print("=============================================")
    print(df_final.count())  # 8165
    # expected 8703
    print("=============================================")
    # # time.sleep(120)
    spark.stop()
