from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, LongType
import datetime

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("Load and Print Processed Logs") \
    .getOrCreate()

# 현재 날짜를 가져오기 (YYYY-MM-DD 형식)
current_date = datetime.datetime.now().strftime('%Y-%m-%d')

# Step 1에서 저장된 데이터 경로
step1_output_path = f"s3a://tripcok/preprocessed_data/{current_date}/step1/url=http%3A%2F%2Ftr-sv-1%3A9090%2Fapi%2Fv1%2Fgroup%2Fpost/"

# 데이터 스키마 정의
schema = StructType([
    StructField("traceId", StringType(), True),
    StructField("method", StringType(), True),
    StructField("time", LongType(), True),
    StructField("url", StringType(), True),
    StructField("statusCode", StringType(), True),
    StructField("clientIp", StringType(), True),
    StructField("memberId", StringType(), True),
    StructField("request", StringType(), True),
    StructField("response", StringType(), True)
])

# Step 1에서 저장된 데이터를 읽기
try:
    processed_data = spark.read.parquet(step1_output_path)
    
    # 읽어온 데이터 출력
    print("************ Loaded Data ************")
    processed_data.show(n=100,truncate=False)  # 모든 열의 데이터를 출력

    # 데이터의 행 수 출력
    print(f"Row count: {processed_data.count()}")

except Exception as e:
    print(f"Error while loading data: {e}")

finally:
    # 작업 완료 후 Spark 세션 종료
    spark.stop()
