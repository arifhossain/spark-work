from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder \
    .appName("hello-spark-sql") \
    .getOrCreate()


def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=fields[1], age=int(fields[2]), friends=int(fields[3]))

mydatafile = spark.sparkContext.textFile("data/fakeFriends.csv")
people = mydatafile.map(mapper)

dataframe = spark.createDataFrame(people)
    # .createOrReplaceTempView("people")

# teens = spark.sql("select * from people where age>=13 and age<=19")
# teens.show()
dataframe.groupBy("age").count().orderBy("age").show()