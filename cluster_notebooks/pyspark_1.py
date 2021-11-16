from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from config.utils import get_spark_app_config
from config.utils import load_df
from config.utils import count_by_country


conf = get_spark_app_config()

print("#### Spark Session creating using config file")
spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()
print("#### Spark Session created using config file")

## this will be in JOB 0 AND JOB 1
## reading dataframe
print("#### reading dataframe")
path = r'data/sample.csv'
df = load_df(spark,path)

## this will result in JOB2 till show() method
partitioned_df = df.repartition(2)
#df.printSchema()


count_df = count_by_country(df)

print(count_df.collect())
#count_df.show()
input("press enter")
