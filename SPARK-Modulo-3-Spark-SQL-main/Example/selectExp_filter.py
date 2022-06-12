from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, col, expr
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

if __name__ == '__main__':
    path = "/tmp/data/workspace/Data - Laboratorio/ExportCSV.csv"
    spark = SparkSession.builder.appName('SelectFilter').getOrCreate()
    sc = spark.sparkContext
    # Reading main data
    df_with_nulls = spark.read.format("csv").option("header", "true").load(path)
    # Dropping nulls
    df_cleaned = df_with_nulls.na.drop()

    # select nested struct
    data = [
        (("James", None, "Smith"), "OH", "M"),
        (("Anna", "Rose", ""), "NY", "F"),
        (("Julia", "", "Williams"), "OH", "F"),
        (("Maria", "Anne", "Jones"), "NY", "M"),
        (("Jen", "Mary", "Brown"), "NY", "M"),
        (("Mike", "Mary", "Williams"), "OH", "M")
    ]

    schema = StructType([
        StructField(
            'name', StructType([
                StructField('firstname', StringType(), True),
                StructField('middlename', StringType(), True),
                StructField('lastname', StringType(), True)
            ])),
        StructField('state', StringType(), True),
        StructField('gender', StringType(), True)
    ])

    df2 = spark.createDataFrame(data=data, schema=schema)
    df2.printSchema()
    df2.show(truncate=False)  # shows all columns

    df2.select("name").show(truncate=False)
    df2.select("gender").show(truncate=False)
    df2.select("name.firstname", "name.lastname").show(truncate=False)
    df2.select("name.*").show(truncate=False)

    # SELECT EXPRESION
    df = df_cleaned.withColumn("Date", unix_timestamp(col("Date"), "M/d/yyyy h:mm a").cast(TimestampType()))

    # in which trimester, is the month placed ?
    df_cleaned2 = df.selectExpr("*",  # Select what columns you would like to show
                                "case "
                                "when (MONTH(Date)<=4 and MONTH(Date)>=1) then concat(YEAR(Date),'Q1')"
                                " when (MONTH(Date)<=8 and MONTH(Date)>=5) then concat(YEAR(Date),'Q2') "
                                "when (MONTH(Date)<=12 and MONTH(Date)>=9) then concat(YEAR(Date),'Q3') "
                                "ELSE concat(YEAR(Date),'BAD') "
                                "end AS trimester")

    df.selectExpr("*", "Date", "State").show()
    # Select with expr
    df.select(col("Date"), col("ID"), expr("add_months(to_date(Date,'yyyy-MM-dd'), 2)").alias("adding_months")).show()

    df_cleaned2.show()
    spark.stop()
