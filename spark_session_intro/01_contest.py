from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IntroToSpark").getOrCreate()

print(spark)