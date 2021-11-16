## program for columnar transformations


from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder \
    .master("local") \
    .getOrCreate()

df = spark.read \
    .format("csv") \
    .option("header","true") \
    .option("path",r"data/sample.csv") \
    .load()

df.show(5,False)


## column objects are always used with transformations
## 2 ways to refer to columns in dataframe transformations 1. column string 2. column object

# column string
filtered_df = df.select("Timestamp","Age","Gender","Country","no_employees")
filtered_df.show(5,False)
filtered_df.printSchema()

filtered_df.select(col('Country'), filtered_df.Gender).show(5, False)

## column expressions in 2 ways: 1. string expressions / sql expressions, 2. Column object expressions
filtered_df.select("Timestamp", expr("to_date(Timestamp) as date")).show(5,False)
filtered_df.select('Timestamp',to_date('Timestamp').alias('date')).show(5,False)