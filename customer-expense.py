from pyspark import SparkConf, SparkContext
import collections

def parseLine(line):
    fields = line.split(',')
    return (fields[0], float(fields[2]))

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

line = sc.textFile("data/customer-orders.csv")
rdd = line.map(parseLine)
rdd = rdd.reduceByKey(lambda x,y: x+y)

result = rdd.collect()

for item in result:
    print item
