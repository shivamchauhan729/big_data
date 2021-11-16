from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder \
    .master("local") \
    .getOrCreate()


df = spark.read \
    .format('csv') \
    .option("header","true") \
    .option("path","data/invoices.csv") \
    .load()

## single value aggregates

# when taking count of dataset, always use count(*) and not the count on some field, because
# counting the field values ignores the null but not in the case of whole dataset *

df.select(count('*').alias("count"),
        sum("Quantity").alias("total quantity"),
        avg("UnitPrice").alias("Avg Price"),
        countDistinct('InvoiceNo').alias('CountDistinct')).show()


df.selectExpr("count(*) as count",
           "sum(Quantity) as totalquantity ",
           "avg(UnitPrice) as AvgPrice").show()


df.createOrReplaceTempView('sales')

spark.sql("""select Country, 
        InvoiceNo,
        sum(Quantity) as TotalQuantity,
        round(sum(Quantity*UnitPrice),2) as InvoiceValue
    from sales
    group by Country,InvoiceNo""").show()


df.groupBy('Country','InvoiceNo') \
    .agg(sum('Quantity').alias('TotalQuantity'), \
        round(sum(col('Quantity')*col('UnitPrice')),2).alias('InvoiceValue')).show()


spark.sql("""select Country, weekofyear(cast(InvoiceDate as date)) as WeekNumber,
        count(*) as NumberInvoices,
        sum(Quantity) as TotalQuantity
    from sales
    group by Country, weekofyear(cast(InvoiceDate as date))""").show()


