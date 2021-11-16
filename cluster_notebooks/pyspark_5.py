from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import re

from pyspark.sql.types import StringType
## learn to use udf 

spark = SparkSession.builder \
    .master("local") \
    .getOrCreate()


df = spark.read \
    .format('csv') \
    .option("path",r"data/survey.csv") \
    .option("header","true") \
    .option("inferSchema","true") \
    .load()

#df.show(5)

## register this custom function as udf function to the driver
def parse_gender(gender):
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"^m$|ma|m.l"

    if re.search(female_pattern,gender.lower()):
        return "Female"
    elif re.search(male_pattern,gender.lower()):
        return "Male"
    else:
        return "Unknown"

## registering your fuction as a dataframe udf, it will not register the udf in catalog
## only serialize the function to the executors

## udf() function will register parse_gender and returns a udf_reference, you can then use
## udf_reference in string or column expressions, now your udf is registered in spark session
## and driver will serialize this function to the executors
parse_gender_udf = udf(parse_gender, StringType())  
print("spark catalog function : ",[f for f in spark.catalog.listFunctions() if "parse_gender" in f.name])
df.select("Gender",parse_gender_udf('Gender').alias("Correct Gender")).show(5)


## if you want to use your custom function in a sql like expression
## then you must register it like this
spark.udf.register("parse_gender_udf",parse_gender,StringType())
print("spark catalog function : ",[f for f in spark.catalog.listFunctions() if "parse_gender" in f.name])

df.select("Gender",expr("parse_gender_udf(Gender)").alias("Correct Gender")).show(5)