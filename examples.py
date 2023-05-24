import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StringType

# Define a UDF to convert byte arrays to strings
def byte_array_to_string(byte_array):
    return byte_array.decode('utf-8')
byte_array_to_string_udf = udf(byte_array_to_string, StringType())
# Tạo SparkSession
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

# Đọc dữ liệu từ Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("startingOffsets", "earliest") \
    .option("subscribe", "Shop") \
    .load()

# In dữ liệu từ Kafka
query = df.select(byte_array_to_string_udf("value"))\
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
time.sleep(10)
spark.stop()
# # Chờ cho quá trình in ra màn hình kết thúc
# query.awaitTermination()
# spark.stop()