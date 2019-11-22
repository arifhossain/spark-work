from pyspark import SparkConf, SparkContext
import collections

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    friends = int(fields[3])
    return (age, friends)

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

line = sc.textFile("social-network.data")
rdd = line.map(parseLine)
rdd = rdd.mapValues(lambda x: (x,1))
rdd = rdd.reduceByKey(lambda x,y : (x[0] + y[0], x[1] + y[1]))
rdd = rdd.mapValues(lambda x: x[0]/x[1])
result = rdd.collect()

for item in result:
    print item
