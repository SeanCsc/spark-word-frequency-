# [莎士比亚文集词频统计并行化算法]
Spark入门项目

## 项目描述
在给定的莎士比亚文集上，根据停词表，统计出现频率最高的100个词

输入：多个文件

输出：出现频次最高的100个单词，输出文件中每个单词一行


## Spark编程模型
开发的应用程序都是由driver programe构成，该驱动程序通过跑main函数来执行各种并行操作。

并行计算访问的元素集合：RDD（弹性分布式数据集）。操作包括转换（比如map）和动作（比如reduce)

## 编程语言及库函数
本次项目基于Python实现，利用pyspark库并行实现。
```python
from pyspark import SparkContext,SparkConf
from operator import add
```

## 开发流程：
#### 首先创建SparkConf对象，包括一系列应用信息（应用程序名，主节点URL等）->创建SparkContext(让Spark知道如何连接Spark集群）
```python
appName = "WordCount"
conf = SparkConf().setAppName(appName).setMaster("local")
sc = SparkContext(conf=conf)
```
#### 创建RDD
本项目中通过sc.textFile()创建RDD
(创建RDD可以从文件系统或者HDFS中的文件中创建，也可以从Python的集合中创建）
```python
inputRDD = sc.textFile(inputFiles)
stopRDD = sc.textFile(stopWordFile)
```
#### 预处理
-排除非单词干扰。因为该项目要求为统计单词的个数，因此先将标点符号等干扰去除。因此使用空格替换这些标点符号，同时将替换后的行拆分成单词。
```python
targetList = list('\t\().,?[]!;|') + ['--']
def replaceAndSplit(s):
    for c in targetList:
        s = s.replace(c, " ")
    return s.split()
inputRDDv1 = inputRDD.flatMap(replaceAndSplit)
```
-处理停词表
去除空行并获得单词表
```python
stopList = stopRDD.map(lambda x: x.strip()).collect()
```
-去除文件中的停词
```python
inputRDDv2 = inputRDDv1.filter(lambda x: x not in stopList)
```
#### Map
将每个单词map到一个元组(word,1)
```python
map(lambda x:(x,1))
```
#### Reduce
根据上一步中的元组，按照单词（key)将value相加。
```python
reduceByKey(operation)
saveAsTextFile('/tmp/v4output')
```
#### TopK
-交换key/value位置

-排序

-提取所有单词（去掉频次信息）

-获取100

```python
inputRDDv5 = inputRDDv4.map(lambda x: (x[1], x[0]))
inputRDDv6 = inputRDDv5.sortByKey(ascending=False)
inputRDDv7 = inputRDDv6.map(lambda x: (x[1], x[0])).keys()
top100 = inputRDDv7.take(100)
```
#### 存储
```python
outputFile = "/tmp/result"
result = sc.parallelize(top100)
result.saveAsTextFile(outputFile)
```
#### 执行文件
spark submit

#### 结果示例
![result](https://github.com/SeanCsc/spark-word-frequency-/blob/master/other/out.jpg)





