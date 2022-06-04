from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, isnull, unix_timestamp, when, bround, avg,min,max,sum, udf
from pyspark.sql.types import TimestampType, DecimalType, StringType

if __name__ == '__main__':   
    new_path = "REALTIME_ADEL_SUELDO_ORIGINAL.TXT"
    spark = SparkSession.builder.appName('Examples').getOrCreate()
    spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", "\\t").load(
        new_path).show()
    spark.stop()