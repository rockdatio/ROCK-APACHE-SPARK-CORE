from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

if __name__ == '__main__':
    path = "/tmp/data/workspace/Data - Laboratorio/ExportCSV.csv"
    spark = SparkSession.builder.appName('SelectFilter').getOrCreate()
    sc = spark.sparkContext
    # Reading main data
    df_with_nulls = spark.read.format("csv").option("header", "true").load(path)
    # Dropping nulls
    df_cleaned = df_with_nulls.na.drop()

    def removeDashChar(entry: str):
        """
        This function removes all ocurrences of a dash
        character of an input string
        """
        return entry.replace("-", "")


    # df.select(df["Social Security Number"]).show()
    removeDashCharUDF = udf(lambda z: removeDashChar(z), StringType())
    df = df_cleaned.withColumn("Social Security Number", removeDashCharUDF(col("SSN")))
    # df = df_cleaned.withColumn("Social Security Number", removeDashCharUDF(col("Social Security Number")))
    df.select(df["SSN"]).show()
    df.select(df["Social Security Number"]).show()
    # df.select(df["Social Security Number"]).show()
    spark.stop()
