from pyspark.sql import SparkSession
#import avro.schema

if __name__ == '__main__':
    new_path = "/tmp/data/workspace/Data - Laboratorio/ExportCSV.csv"
    spark = SparkSession.builder.appName('Examples').getOrCreate()
        # .config('spark.jars', 'com.databricks:spark-avro_2.11:2.0.1') \
    spark.read.format("csv").option("header", "true").load(new_path)

    new_path1 = "/tmp/data/workspace/Data - Laboratorio/zipcodes.csv"
    new_path2 = "/tmp/data/workspace/Data - Laboratorio/zipcodes.json"
    new_path3 = "/tmp/data/workspace/Data - Laboratorio/zipcodes.avro"
    new_path4 = "/tmp/data/workspace/Data - Laboratorio/zipcodes.parquet"

    spark.read.format("csv").load(new_path1).show(20)
    spark.read.format("json").load(new_path2).show(20)
    spark.read.format("parquet").load(new_path4).show(20)
    # IMPLICIT FORM
    spark.read.format("avro").load(new_path3).show(20)

    # EXPLICIT FORM
    # schemaCity = """{
    #                      "type" : "record",
    #                      "name" : "name1",
    #                      "namespace" : "com.data",
    #                      "fields" : [
    #                      {
    #                        "name": "RecordNumber",
    #                        "type": "int"
    #                      },
    #                      {
    #                        "name": "Zipcode",
    #                        "type": "int"
    #                      },
    #                      {
    #                        "name": "ZipCodeType",
    #                        "type": "string"
    #                      },
    #                      {
    #                        "name": "City",
    #                        "type": "string"
    #                      },
    #                      {
    #                        "name": "State",
    #                        "type": "string"
    #                      },
    #                      {
    #                        "name": "LocationType",
    #                        "type": "string"
    #                      },
    #                      {
    #                        "name": "Lat",
    #                        "type": "double"
    #                      },
    #                      {
    #                        "name": "Long",
    #                        "type": "double"
    #                      }
    #                      ]
    #                    }"""
    #
    # # schema = avro.schema.parse(open("extra_data/schema_zipcode.avsc", "rb").read())
    # # schema = avro.schema.parse(schemaCity)
    # spark.read.format("com.databricks.spark.avro").option("avroSchema", schemaCity).load(new_path3).show()
    # spark.read.format("avro").option("avroSchema", schemaCity).load(new_path3).show()

    spark.stop()
