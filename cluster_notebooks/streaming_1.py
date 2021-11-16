from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession \
    .builder \
    .appName("streaming application") \
    .master("local") \
    .config("spark.streaming.stopGracefullyOnShutdown","true") \
    .config("spark.sql.streaming.schemaInference","true") \
    .getOrCreate()

## reading into streaming dataframe

raw_df = spark.readStream \
    .format("json") \
    .option("path","data") \
    .option("maxFilesperTrigger","1") \
    .option("cleanSource","archive") \
    .option("sourceArchiveDir","archived_files") \
    .load()


## process the streaming dataframe

## schema of this dataframe
##raw_df.printSchema()

explode_df = raw_df.selectExpr("InvoiceNumber","CreatedTime","StoreID","PosID","CustomerType", 
"PaymentMethod","DeliveryType","DeliveryAddress.City","DeliveryAddress.State",
"DeliveryAddress.PinCode","explode(InvoiceLineItems) as LineItems")

explode_df.printSchema()

flattened_df = explode_df \
    .withColumn("ItemCode",col("LineItems.ItemCode")) \
    .withColumn("ItemDescription",col("LineItems.ItemDescription")) \
    .withColumn("ItemPrice",col("LineItems.ItemPrice")) \
    .withColumn("ItemQty",col("LineItems.ItemQty")) \
    .withColumn("ItemValue",col("LineItems.TotalValue")) \
    .drop("LineItems")

## writer query
#flattened_df.printSchema()

invoice_writer_query = flattened_df.writeStream \
    .format("json") \
    .option("path","output") \
    .option("checkpointLocation","chk-point-dir") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start()

invoice_writer_query.awaitTermination()