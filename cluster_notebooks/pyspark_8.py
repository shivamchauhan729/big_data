from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder \
    .master("local") \
    .getOrCreate()


orders_list = [("01", "02", 350, 1),
                   ("01", "04", 580, 1),
                   ("01", "07", 320, 2),
                   ("02", "03", 450, 1),
                   ("02", "06", 220, 1),
                   ("03", "01", 195, 1),
                   ("04", "09", 270, 3),
                   ("04", "08", 410, 2),
                   ("05", "02", 350, 1)]

order_df = spark.createDataFrame(orders_list,['order_id','prod_id','unit_price','qty'])
order_df.show()

product_list = [("01", "Scroll Mouse", 250, 20),
                    ("02", "Optical Mouse", 350, 20),
                    ("03", "Wireless Mouse", 450, 50),
                    ("04", "Wireless Keyboard", 580, 50),
                    ("05", "Standard Keyboard", 360, 10),
                    ("06", "16 GB Flash Storage", 240, 100),
                    ("07", "32 GB Flash Storage", 320, 50),
                    ("08", "64 GB Flash Storage", 430, 25)]

product_df = spark.createDataFrame(product_list,("prod_id", "prod_name", "list_price", "qty"))
product_df.show()


order_df.join(product_df,order_df.prod_id==product_df.prod_id,"inner") \
        .select("order_id","prod_name","unit_price",order_df.qty).show()
## every dataframe column has a unique id in the catalog, and spark engine always works 
# using these internal ids and these internal ids are not shown to us and we are expected to
# work with the column names
# however spark engine translates this column names to ids during the analysis phase
# so that's why when i select the column name with the same name in both join tables,
# it gives ambiguity error as spark engine isn't abe to translate the column name into unique id
# but when we do select *, this time spark engine directly interacts with unique ids and 
# so translation of column name to id is not needed in this case




order_df.join(product_df,order_df.prod_id==product_df.prod_id,"outer") \
        .select("order_id","prod_name","unit_price",order_df.qty) \
        .sort('order_id').show()


order_df.join(product_df,order_df.prod_id==product_df.prod_id,"left") \
        .select("order_id","prod_name","unit_price",order_df.qty) \
        .sort('order_id').show()

## coalesce returns the first non null value

order_df.join(product_df,order_df.prod_id==product_df.prod_id,"left") \
        .select("order_id","prod_name","unit_price",order_df.qty) \
        .withColumn("prod_name",coalesce("prod_name","order_id")) \
        .sort('order_id').show()
