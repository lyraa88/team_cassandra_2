from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# SparkSession 초기화
spark = SparkSession.builder \
    .appName("LogProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("ERROR")

# Kafka에서 데이터 읽기 위한 스키마 정의
schema = StructType([
    StructField("accessed_date", StringType(), True),
    StructField("duration_(secs)", IntegerType(), True),
    StructField("network_protocol", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("bytes", IntegerType(), True),
    StructField("accessed_Ffom", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("country", StringType(), True),
    StructField("membership", StringType(), True),
    Field("language", StringType(), True),
    StructField("sales", DoubleType(), True),
    StructField("returned", StringType(), True),
    StructField("returned_amount", DoubleType(), True),
    StructField("pay_method", StringType(), True),
    StructField("item_category", StringType(), True),
])

# Kafka로부터 스트리밍 데이터 읽기
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "e-commerce-logs") \
    .load()

# JSON 값을 파싱하고 데이터프레임 구조화
query_stream = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 콘솔에 결과 출력
query = query_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()