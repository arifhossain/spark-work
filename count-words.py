
from pyspark import SparkConf, SparkContext
import collections

def parseLine(line):
    fields = line.split(',')
    return (fields[0], fields[2], float(fields[3]))

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

lines = sc.textFile("sample.txt")
words = lines.flatMap(lambda x: x.replace('.', '').split(' '))
rdd = words.map(lambda x: (x,1))
rdd = rdd.reduceByKey(lambda x,y:x+y)
rdd = rdd.map(lambda (x,y): (y,x)).sortByKey()

result = rdd.collect()

for item in result:
    print item 