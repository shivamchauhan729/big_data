from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id
from pyspark.sql.types import DateType, IntegerType, StructField, StructType
from config.utils import get_spark_app_config


conf = get_spark_app_config()

spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()


df = spark.read \
    .format('csv') \
    .option("header","true") \
    .option("path",r"data/flight-time.csv") \
    .option("inferSchema","true") \
    .load()

df.show(5)

print(df.schema.simpleString())
#df.printSchema()
df.groupBy(spark_partition_id()).count().show()
df.write.format("csv").mode("overwrite").option("path",r"output/tempfile") \
   .partitionBy("OP_CARRIER","ORIGIN") \
   .option("maxRecordsPerFile",10000) \
   .save()       # -- this will limit the file size to 10000 and 
                    #create another partition if data size orginally exceeds this no.


schema_df = StructType([StructField('FL_DATE',DateType()),
            StructField('OP_CARRIER_FL_NUM',IntegerType())
])

schema_ddl = "FL_DATE DATE, OP_CARRIER_FL_NUM INT"


df1 = spark.read \
    .format('csv') \
    .option('header','true') \
    .option('path',r'data/flight-time.csv') \
    .schema(schema_ddl) \
        .load()
 #   .option("dataFormat","M/d/y") \
     #.option("mode","FAILFAST")
## if found any malformed record like in this case, no. of column dont match with source file, then throw an exception
## other modes are permissive (default, in this fields are set to null) and dropmalformed (ignores the malformed records)    

print(df1.schema.simpleString())
df1.show(5)