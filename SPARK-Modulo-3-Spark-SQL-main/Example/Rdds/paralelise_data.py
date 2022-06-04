from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, isnull, unix_timestamp, when, bround, avg, min, max, sum, udf
from pyspark.sql.types import TimestampType, DecimalType, StringType


if __name__ == '__main__':
    # ==========================
    # 1.- Starting spark session
    # ==========================

    # spark = SparkSession.builder.master("local[1]").appName('Examples').config('spark.jars.packages','org.apache.spark:spark-avro_2.12:3.1.2').getOrCreate()
    spark = SparkSession.builder.appName('Examples').getOrCreate()
    sc = spark.sparkContext

    data = [1, 2, 3, 4, 5]
    distData = sc.parallelize(data, 180)
    print(distData.getNumPartitions())

    data2 = [1, 2, 3, 4, 5]
    dist_data2 = sc.parallelize(data)
    print(dist_data2.getNumPartitions())

    distFile = sc.textFile("/tmp/data/workspace/Data - Laboratorio/diabetes.cvs")

    spark.stop()
