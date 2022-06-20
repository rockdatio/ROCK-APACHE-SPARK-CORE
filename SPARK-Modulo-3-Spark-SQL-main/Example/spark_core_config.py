from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName('TIME').getOrCreate()
    spark.conf.set("spark.executor.memory", '8g')
    spark.conf.set('spark.executor.cores', '3')
    spark.conf.set('spark.cores.max', '3')
    spark.conf.set("spark.driver.memory", '8g')
    sc = spark.sparkContext

    spark.stop()
