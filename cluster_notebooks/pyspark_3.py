from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder \
    .master("local") \
    .getOrCreate()

df = spark.read.text(r"data/apache_logs.txt")

df.printSchema()

reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'
log_df = df.select(regexp_extract('value', reg, 1).alias("IP"))

log_df = df.select(regexp_extract('value', reg, 10).alias('referrer'))
log_df.show(5,False)

log_df = log_df.select('referrer',substring_index('referrer',"/", 3).alias("homepage"))
log_df.show(5,False)

log_df.where("trim(homepage) != '-'").groupBy('homepage').count().show(5,False)
