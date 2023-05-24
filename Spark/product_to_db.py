import time
from pyspark.sql import SparkSession
import json
from pyspark.sql.functions import udf,split
from pyspark.sql.types import StringType,IntegerType,FloatType


def deserialize(value):
    str_value = value.decode("utf-8")
    return str_value

# Define a UDF to convert byte arrays to strings
def byte_array_to_string(byte_array):
    return byte_array.decode('utf-8')
byte_array_to_string_udf = udf(byte_array_to_string, StringType())

scala_version = '2.12'
spark_version = '3.3.1'
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}'

]
spark = SparkSession.builder\
   .master("local")\
   .appName("kafka-example")\
   .config("spark.driver.memory", "2g")\
   .config("spark.executor.memory", "2g")\
   .config("spark.jars.packages", ",".join(packages))\
   .getOrCreate()
spark.udf.register("deserialize", deserialize, StringType())

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "Product") \
    .option("startingOffsets", "earliest") \
    .option("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")\
    .option("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")\
   .load()\

valueDf = df.withColumn("value", byte_array_to_string_udf("value"))
valueDf = df.selectExpr("deserialize(value) as value")

print(valueDf)

splitDf = valueDf.select(
    split(valueDf["value"], ",").getItem(0).cast(StringType()).alias("productID"),
    split(valueDf["value"], ",").getItem(1).cast(StringType()).alias("product_name"),
    split(valueDf["value"], ",").getItem(2).cast(StringType()).alias("brandID"),
    split(valueDf["value"], ",").getItem(3).cast(StringType()).alias("original_price"),
    split(valueDf["value"], ",").getItem(4).cast(StringType()).alias("discount"),
    split(valueDf["value"], ",").getItem(5).cast(StringType()).alias("price"),
    split(valueDf["value"], ",").getItem(6).cast(StringType()).alias("discount_percent"),
    split(valueDf["value"], ",").getItem(7).cast(StringType()).alias("rating"),
    split(valueDf["value"], ",").getItem(8).cast(StringType()).alias("review_count"),
    split(valueDf["value"], ",").getItem(9).cast(StringType()).alias("quantity_sold"),
    split(valueDf["value"], ",").getItem(10).cast(StringType()).alias("shopID"),
    split(valueDf["value"], ",").getItem(11).cast(StringType()).alias("dateID")
)
# # splitDf.head()
# print(type(splitDf))

splitDf.write \
    .mode("append") \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/baocao") \
    .option("dbtable", "FactSaleProduct") \
    .option("user", "postgres") \
    .option("password", "1410") \
    .option("driver", "org.postgresql.Driver") \
    .save()
# Ghi splitDf vào file CSV
# query = splitDf.writeStream \
#     .format("csv") \
#     .option("header", "true")\
#     .option("path", "C:/Users/ASUS/Desktop/CNPM/demo/DWH")\
#     .option("checkpointLocation", "C:/Users/ASUS/Desktop/CNPM/demo/DWH/checkpoint")\
#     .start()

# query.awaitTermination()
# query.stop()


