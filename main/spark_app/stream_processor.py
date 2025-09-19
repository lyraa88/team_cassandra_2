from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, udf, to_date, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType
import uuid

# UUID 생성 UDF 정의
def generate_unique_uuid(ip, accessed_date):
    combined_data = f"{ip}-{accessed_date}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, combined_data))

generate_uuid_udf = udf(generate_unique_uuid, StringType())

# SparkSession 초기화
spark = SparkSession.builder \
    .appName("LogProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Kafka 메시지 스키마 정의 (모든 필드를 StringType으로 정의)
schema = StructType([
    StructField("accessed_date", StringType(), True),
    StructField("duration_(secs)", StringType(), True),
    StructField("network_protocol", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("bytes", StringType(), True),
    StructField("accessed_Ffom", StringType(), True),
    StructField("age", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("country", StringType(), True),
    StructField("membership", StringType(), True),
    StructField("language", StringType(), True),
    StructField("sales", StringType(), True),
    StructField("returned", StringType(), True),
    StructField("returned_amount", StringType(), True),
    StructField("pay_method", StringType(), True),
    StructField("item_category", StringType(), True),
])

# Kafka로부터 스트리밍 데이터 읽기 (logs_raw 토픽에서 읽음)
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "logs_raw") \
    .load()

# JSON 값을 파싱하고 데이터프레임 구조화
preprocessed_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# 칼럼명 수정: accessed_Ffom -> accessed_Form
preprocessed_df = preprocessed_df.withColumnRenamed("accessed_Ffom", "accessed_from")

# 유효 ID(UUID) 생성
preprocessed_df = preprocessed_df.withColumn("accessed_at", generate_uuid_udf(col("ip"), col("accessed_date")))

# 날짜에서 시간 정보 제거 (to_date 함수 사용)
preprocessed_df = preprocessed_df.withColumn("log_date", to_date(to_timestamp(col("accessed_date"), "yyyy-MM-dd HH:mm:ss.SSS")))

# logs_clean 토픽에 쓰기
query = preprocessed_df.select(to_json(struct(col("*"))).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "logs_clean") \
    .option("checkpointLocation", "/tmp/spark/logs_clean_checkpoint") \
    .start()

# 스트림 대기
query.awaitTermination()