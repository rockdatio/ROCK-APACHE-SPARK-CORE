from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import date_format

if __name__ == '__main__':
    spark = SparkSession.builder.appName('TIME').getOrCreate()
    # spark.conf.set("spark.executor.memory", '8g')
    # spark.conf.set('spark.executor.cores', '3')
    # spark.conf.set('spark.cores.max', '3')
    # spark.conf.set("spark.driver.memory", '8g')
    sc = spark.sparkContext

    inputData = [("2019-07-01 12:01:19",
                  "07-01-2019 12:01:19",
                  "07-01-2019",
                  "2028-06-02T23:40:48.314-0500",
                  # "",
                  "2022-06-02T23:30:00.045243-05:00",
                  "2021-08-03T16:15:20.000000509-05:00"
                  )]
    columns = ["timestamp_1", "timestamp_2", "timestamp_3", "timestamp_4", "timestamp_5", "timestamp_6"]
    df = spark.createDataFrame(
        data=inputData,
        schema=columns)
    df.printSchema()
    df.show(truncate=False)

    df2 = df .select(
        unix_timestamp(col("timestamp_1")).alias("timestamp_1"),
        unix_timestamp(col("timestamp_2"), "MM-dd-yyyy HH:mm:ss").alias("timestamp_2"),
        unix_timestamp(col("timestamp_3"), "MM-dd-yyyy").alias("timestamp_3"),
        col("timestamp_4").alias("timestamp_4"),
        date_format(col("timestamp_5"), "MM-dd-yyyy").alias("timestamp_5"),
        date_format(col("timestamp_6"), "MM-dd-yyyy").alias("timestamp_6"),
        unix_timestamp().alias("timestamp_7")
    ).withColumn("requestDate", when((col("timestamp_4").isNull() | (col("timestamp_4") == '')), "null").otherwise(substring(col("timestamp_4"), 1, 10)))
    df2.printSchema()

    df2.show(truncate=False)
    df2.write.format("parquet").mode("append").partitionBy("requestDate").save("/tmp/data/hadoop/landing/badi2/")

    # df3 = df2.select(
    #     from_unixtime(col("timestamp_1")).alias("timestamp_1"),
    #     from_unixtime(col("timestamp_2"), "MM-dd-yyyy HH:mm:ss").alias("timestamp_2"),
    #     from_unixtime(col("timestamp_3"), "MM-dd-yyyy").alias("timestamp_3"),
    #     from_unixtime(col("timestamp_4")).alias("timestamp_4")
    # )
    # df3.printSchema()
    # df3.show(truncate=False)

    spark.stop()
