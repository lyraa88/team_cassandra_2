from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# SparkSession 초기화
spark = SparkSession.builder \
    .appName("LogProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Kafka 메시지 읽기 (logs_raw 토픽에서 읽음)
# 전처리 없이 원시 데이터를 바로 읽습니다.
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "logs_raw") \
    .load()

# 읽어온 'value' 컬럼을 바로 logs_clean 토픽에 씁니다.
# from_json 등의 파싱 로직을 제거했습니다.
query = raw_df.select(col("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "logs_clean") \
    .option("checkpointLocation", "/tmp/spark/logs_clean_checkpoint") \
    .start()

# 스트림 대기
query.awaitTermination()