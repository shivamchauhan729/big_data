from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType


spark = SparkSession.builder \
        .master("local") \
        .getOrCreate()


data_list = [("Ravi", "28", "1", "2002"),
                 ("Abdul", "23", "5", "81"),  # 1981
                 ("John", "12", "12", "6"),  # 2006
                 ("Rosy", "7", "8", "63"),  # 1963
                 ("Abdul", "23", "5", "81")]  # 1981

spark.createDataFrame(data_list).toDF("name","day","month","year").show()

df = spark.createDataFrame(data_list,("name","day","month","year"))

## partitions in which dataframe is distributed
print("number of partitions : ", df.rdd.getNumPartitions())
modified_df = df.withColumn("id_when_1_partition",monotonically_increasing_id())
modified_df.show()

# partitions increased
partitioned_df = modified_df.repartition(3)
print("number of partitions after increasing partitions: ",partitioned_df.rdd.getNumPartitions())
partitioned_df.withColumn("id when multiple_partitions",monotonically_increasing_id()).show()


## switch case statements
partitioned_df.printSchema()
partitioned_df \
        .withColumn("corrected year",expr("case when year < 21 then year + 2000 when year < 100 \
                 then year + 1900 else year end").cast(IntegerType())).show()

partitioned_df.withColumn('corrected_year',when(col('year')<21, col('year')+2000) \
                .when(col('year')<100, col('year')+1900) \
                .otherwise(col('year'))).show()


df.withColumn('dob',expr("concat(day,'/',month,'/',year)")).show()

df.withColumn('dob',to_date(expr("concat(day,'/',month,'/',year)"),'d/M/y')).show()

df1 = df.withColumn('dob',to_date(concat_ws('/','day','month','year'),'d/M/y'))

df1.drop_duplicates(['name','dob']).show()

df1.sort(expr("day desc")).show()
