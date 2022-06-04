import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, when, unix_timestamp
from pyspark.sql.types import TimestampType, DecimalType, StringType

if __name__ == '__main__':
    data = "/tmp/data/workspace/Data - Laboratorio/ExportCSV.csv"
    spark = SparkSession.builder.appName('TIME').getOrCreate()
    df_with_nulls = spark.read.format("csv").option("header", "true").load(data)
    df_cleaned = df_with_nulls.na.drop()

    print("=============================================")
    df_cleaned.printSchema()
    # expected 8703
    print("=============================================")

    df1 = df_cleaned \
        .withColumn(
        "Date",
        unix_timestamp(df_cleaned["Date"], "M/d/yyyy h:mm a")
            # .cast(TimestampType())
    )
    df1.show()
    df1.printSchema()
    df2 = df_cleaned.withColumn("Purchase (USD)", df_cleaned["Purchase (USD)"].cast(DecimalType(38, 4)))
    df2.show()
    df2.printSchema()
    # df.select(df["Date"]).show()

    # time.sleep(120)
    spark.stop()
