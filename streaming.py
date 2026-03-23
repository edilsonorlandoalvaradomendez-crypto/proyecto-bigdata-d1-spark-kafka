from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder \
    .appName("D1_Streaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("producto", StringType()) \
    .add("precio", IntegerType()) \
    .add("cantidad", IntegerType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ventas_d1") \
    .option("startingOffsets", "latest") \
    .load()

datos = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

conteo = datos.groupBy("producto").count()

query = conteo.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
