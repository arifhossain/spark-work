from pyspark.sql import SparkSession

session = SparkSession.builder \
    .appName("testing spark session") \
    .getOrCreate()

dataFrame = session.read.json("data/people.json")

dataFrame.show()