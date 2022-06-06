import time
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # ==========================
    # 1.- Starting spark session
    # ==========================
    spark = SparkSession.builder.appName('Examples').getOrCreate()
    sc = spark.sparkContext

    ## Cluster 4 nodos y 2 cores = 8 partitions
    # 1 tb => 128 gb de datos por Paritición

    data = [1, 2, 3, 4, 5]
    distData = sc.parallelize(data, 180)
    print(distData.getNumPartitions())

    # 1 nodo y 2 cores => 2 Partions
    data2 = [1, 2, 3, 4, 5]
    dist_data2 = sc.parallelize(data2)
    print(dist_data2.getNumPartitions())
    time.sleep(120)

    distFile = sc.textFile("/tmp/data/workspace/Data - Laboratorio/diabetes.cvs")

    spark.stop()
