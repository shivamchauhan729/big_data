from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("Streaming word count") \
    .config("spark.streamng.stopGracefullyOnShutdown","true") \
    .config("spark.sql.shuffle.partitions",4) \
    .getOrCreate()

lines_df = spark.readStream \
    .format("socket") \
    .option("host","localhost") \
    .option("port","9999") \
    .load()

words_df = lines_df.select(expr("explode(split(value, ' ')) as word"))
counts_df = words_df.groupBy('word').count()

word_count_query = counts_df.writeStream \
    .format("console") \
    .option("checkpointLocation","chk-point-dir") \
    .outputMode("complete") \
    .start()

word_count_query.awaitTermination()