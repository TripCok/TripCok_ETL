from pyspark.sql import SparkSession
from datetime import datetime, timedelta, timezone
import boto3

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("CSV to Parquet Conversion") \
    .getOrCreate()

# S3 클라이언트 생성
s3 = boto3.client('s3')

# S3 버킷과 경로 설정
bucket_name = "tripcok"
csv_prefix = "processed/2024-12-20/"  # CSV가 저장된 경로
parquet_output_prefix = "processed/parquet/2024-12-20/"  # Parquet 저장 경로

# KST 시간대 정의
KST = timezone(timedelta(hours=9))

# S3에서 파일 목록 가져오기
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=csv_prefix)

if 'Contents' in response:
    # CSV 파일 목록 필터링
    csv_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.csv')]

    if csv_files:
        # 전체 CSV 파일 경로 생성
        s3_paths = [f"s3a://{bucket_name}/{key}" for key in csv_files]

        # 모든 CSV 파일을 하나의 DataFrame으로 로드
        df = spark.read.csv(s3_paths, header=True, inferSchema=True)

        # Parquet 저장 경로
        parquet_output_path = f"s3a://{bucket_name}/{parquet_output_prefix}"

        # Parquet로 저장
        df.write.mode("overwrite").parquet(parquet_output_path)

        print(f"Parquet data saved to: {parquet_output_path}")
    else:
        print("No CSV files found.")
else:
    print("No objects found in S3 bucket.")

# Spark 세션 종료
spark.stop()
