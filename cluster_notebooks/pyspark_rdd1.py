from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from config.utils import get_spark_app_config


conf = get_spark_app_config()

spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()

sc = spark.sparkContext

linesRDD = sc.textFile(r'data/sample_without_header.csv')
#print(linesRDD.collect())

partitionedRDD = linesRDD.repartition(2)
colsRDD = partitionedRDD.map(lambda line: line.replace('"','').split(","))
print(colsRDD.collect())

selectRDD = colsRDD.map(lambda cols: (cols[1],cols[2],cols[3],cols[4]))
print(selectRDD.collect())

print("## filtered rdd")
filteredRDD = selectRDD.filter(lambda r: int(r[0])<40)
print(filteredRDD.collect())

print("## key value rdd")
kvRDD = filteredRDD.map(lambda r: (r[2],1))
print(kvRDD.collect())

print("## reduce by key")
countRDD = kvRDD.reduceByKey(lambda a,b: a+b)
print(countRDD.collect())

