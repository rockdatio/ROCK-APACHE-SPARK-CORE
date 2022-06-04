from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, isnull, unix_timestamp, when, bround, avg, min, max, sum, udf
from pyspark.sql.types import TimestampType, DecimalType, StringType

if __name__ == '__main__':
    # new_path = "/tmp/data/workspace/Data - Laboratorio/REALTIME_ADEL_SUELDO_ORIGINAL.TXT"
    new_path = "/tmp/data/workspace/Data - Laboratorio/ExportCSV.csv"
    spark = SparkSession.builder.appName('Examples').getOrCreate()
    spark.read.format("csv").option("header", "true").load(new_path).show(1)

    new_path1 = "/tmp/data/workspace/Data - Laboratorio/zipcodes.csv"
    new_path2 = "/tmp/data/workspace/Data - Laboratorio/zipcodes.json"
    new_path3 = "/tmp/data/workspace/Data - Laboratorio/zipcodes.avro"
    new_path4 = "/tmp/data/workspace/Data - Laboratorio/zipcodes.parquet"

    spark.read.format("csv").load(new_path1).show(1000)
    spark.read.format("json").load(new_path2).show(1000)
    spark.read.format("avro").load(new_path3).show(1000)
    spark.read.format("avro").load(new_path4).show(1000)

    spark.stop()
