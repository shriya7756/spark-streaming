from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

spark = SparkSession.builder \
.appName("RetailStoreStreaming") \
.config("spark.ui.showConsoleProgress","false") \
.getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
StructField("ProductID",IntegerType(),True),
StructField("Amount",DoubleType(),True)
])

input_path = "/home/hadoop/spark_input"
retail_stream_df = spark \
.readStream \
.format("csv") \
.option("header","true") \
.schema(schema) \
.load(input_path)

processed_df = retail_stream_df.groupBy("ProductID").agg(sum("Amount").alias("TotalSales"))

query = processed_df.writeStream \
.outputMode("complete") \
.format("console") \
.start()

query.awaitTermination()