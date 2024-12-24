from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, LongType
from datetime import datetime, timedelta, timezone

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("JSON to CSV Conversion") \
    .getOrCreate()

# KST 시간대 정의
KST = timezone(timedelta(hours=9))

# 현재 KST 날짜 가져오기
current_date = datetime.now(KST).strftime('%Y-%m-%d')

# Step 1에서 저장된 데이터 경로
s3_path = f"s3a://tripcok/topics/tripcoklogs/{current_date}/*.json"

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

# JSON 데이터를 Spark DataFrame으로 읽기
df = spark.read.json(s3_path, schema=schema)

# 플랫하게 변환된 데이터 확인
df.show(truncate=False)

# CSV 저장 경로
csv_output_path = "output/csv_data"

# DataFrame을 CSV로 저장
df.write.mode("overwrite").csv(csv_output_path, header=True)

print(f"CSV 데이터가 {csv_output_path} 경로에 저장되었습니다.")

# Spark 세션 종료
spark.stop()
