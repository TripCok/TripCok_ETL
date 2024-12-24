import boto3
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from datetime import datetime, timedelta, timezone
from pyspark.sql.functions import col, regexp_replace

def getSchema():
    # JSON 데이터의 외부 스키마 정의
    schema = StructType([
        StructField("traceId", StringType(), True),
        StructField("memberId", StringType(), True),
        StructField("clientIp", StringType(), True),
        StructField("url", StringType(), True),
        StructField("method", StringType(), True),
        StructField("request", StringType(), True),  # 문자열 그대로 유지
        StructField("response", StringType(), True),  # 문자열 그대로 유지
        StructField("statusCode", StringType(), True),
        StructField("requestTime", StringType(), True),
        StructField("time", LongType(), True),
    ])
    return schema

def json2df(s3_paths):
    # DataFrame 읽기
    my_schema = getSchema()
    df = spark.read.json(s3_paths, schema=my_schema)
    return df

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("JSON to Parquet Conversion") \
    .getOrCreate()

# S3 클라이언트 생성
s3 = boto3.client('s3')

# S3 버킷과 경로
bucket_name = "tripcok"
prefix = "topics/tripcoklogs/20241220/"  # 처리할 날짜 경로

# KST 시간대 정의
KST = timezone(timedelta(hours=9))

# 현재 KST 날짜 가져오기
current_date = datetime.now(KST).strftime('%Y%m%d')

# S3에서 파일 목록 가져오기
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

if 'Contents' in response:
    # JSON 파일 목록 가져오기
    file_list = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.json')]

    # 전체 경로 생성
    s3_paths = [f"s3a://{bucket_name}/{file_key}" for file_key in file_list]

    # Parquet 저장 경로
    output_path = f"s3a://{bucket_name}/processed_data/{current_date}/L/"

    for path in s3_paths:
        # DataFrame 생성
        print(f"Processing file: {path}")
        df = json2df(path)

        # 데이터 확인 (옵션)
        df.show(truncate=False)

        df = df.withColumn("url", regexp_replace(col("url"), r"^http://[^/]+", ""))
        # Parquet 형식으로 append 저장
        df.coalesce(5).write.mode("append").partitionBy("url","method").parquet(output_path)

    print(f"Batch processing completed. Parquet saved to: {output_path}")
else:
    print("No JSON files found.")

# Spark 세션 종료
spark.stop()
