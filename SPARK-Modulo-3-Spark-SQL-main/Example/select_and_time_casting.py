import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, when, unix_timestamp, from_unixtime
from pyspark.sql.types import TimestampType, DecimalType, StringType

if __name__ == '__main__':
    data = "/tmp/data/workspace/Data - Laboratorio/ExportCSV.csv"

    spark = SparkSession.builder.appName('TIME').getOrCreate()
    # spark.conf.set("spark.sql.session.timeZone", 'America/Los_Angeles')1654979862
    spark.conf.set("spark.sql.session.timeZone", 'Asia/Hong_Kong')  # 1654979973

    sc = spark.sparkContext

    # Reading main data
    df_with_nulls = spark.read.format("csv").option("header", "true").load(data)
    # Dropping nulls
    df_cleaned = df_with_nulls.na.drop()
    df_cleaned.show(5)

    print("=============================================")
    # Show schema
    df_cleaned.printSchema()
    # expected 8703
    print("=============================================")

    # Casting Directly
    df_cleaned \
        .select(col("Date").cast(TimestampType())).withColumn("now", unix_timestamp(format='M/d/yyyy')).show()

    # Using unix_timestamp functions
    df_without_casting = df_cleaned \
        .withColumn(
        "Date",
        unix_timestamp(df_cleaned["Date"], "M/d/yyyy h:mm a")
            .cast(TimestampType())) \
        .withColumn("now", unix_timestamp(format='M/d/yyyy'))

    # What happened without casting ?
    df_without_casting.show()
    df_without_casting.printSchema()

    # Lets transform to better Date format
    # Using from_unixtime functions
    df_time_format = df_without_casting.withColumn("nowDateFormat",
                                                   from_unixtime(df_without_casting["now"], format='yyyy-MM-dd'))
    df_time_format.show()
    df_time_format.printSchema()

    # :param precision: the maximum (i.e. total) number of digits (default: 10)
    # :param scale: the number of digits on right side of dot. (default: 0)
    df2 = df_cleaned.withColumn("Purchase (USD)", df_cleaned["Purchase (USD)"].cast(DecimalType(precision=38, scale=4)))
    df2.show()
    df2.printSchema()
    # df.select(df["Date"]).show()

    # time.sleep(120)
    spark.stop()
