from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, LongType

# SparkSession 생성
spark = SparkSession.builder.appName("READ parquet").getOrCreate()

# S3에서 CSV 파일 로드
parquet_file_path = "s3a://tripcok/processed/20241220/parquet_data/"

parquet = spark.read.parquet(parquet_file_path)

parquet_data.show(truncate=False)


