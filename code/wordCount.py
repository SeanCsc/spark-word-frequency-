from pyspark import SparkContext,SparkConf
from operator import add

appName = "WordCount"
conf = SparkConf().setAppName(appName).setMaster("local")
sc = SparkContext(conf= conf)

inputFiles = "/home/shiyanlou/shakespear/*"
stopWordFile = "/home/shiyanlou/stopword.txt"

outputFile = "/Code/result"

targetList = list('\t\().,?[]!;|')+['--']

def replaceAndSplit(s):
	for c in targetList:
		s = s.replace(c, " ")	
	return s.split()

inputRDD =sc.textFile(inputFiles)
stopRDD =sc.textFile(stopWordFile)

stopList = stopRDD.map(lambda x : x.strip()).collect()

inputRDDv1 = inputRDD.flatMap(replaceAndSplit)
inputRDDv2 = inputRDDv1.filter(lambda x: x not in stopList)
inputRDDv3 = inputRDDv2.map(lambda x: (x,1))
inputRDDv4 = inputRDDv3.reduceByKey(add)
inputRDDv5 = inputRDDv4.map(lambda x:(x[1],x[0]))
inputRDDv6 = inputRDDv5.sortByKey(ascending = False)
inputRDDv7 = inputRDDv6.map(lambda x:(x[1],x[0])).keys()
top100 = inputRDDv7.take(100)

result = sc.parallelize(top100)
result.saveAsTextFile(outputFile)
