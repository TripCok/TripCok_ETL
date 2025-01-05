from pyspark.sql import SparkSession
from common.SparkSess import SessionGen

spark = SessionGen().create_session(app_name="member_place_recommend", local=False)

# S3 Parquet 파일 경로
s3_path = "s3a://tripcok/processed_data/cre_dtm=2025-01-02/"

# Parquet 파일 읽기
df = spark.read.parquet(s3_path)

# 데이터 확인
df.show()