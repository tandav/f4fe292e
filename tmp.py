from pyspark.sql import SparkSession
spark = SparkSession.builder.enableHiveSupport().getOrCreate()


df = spark.sql('select * from sales')
