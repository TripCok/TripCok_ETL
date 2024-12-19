from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from pyspark.sql.utils import AnalysisException

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("Process Logs") \
    .getOrCreate()

# S3 경로와 입력 데이터 설정
s3_path = "s3a://tripcok/topics/logs/2024-12-17/*.json"

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

# JSON 형식으로 데이터 읽기
#data = spark.read.option("multiline", "true").json(s3_path)
df = spark.read.json(s3_path, schema=schema)
#df = spark.read.option("multiline", "true").json(s3_path, schema=schema)

print("************")
# 결과 확인
df.show(truncate=False)  # 모든 열의 내용을 출력

print("*************")

# URL 컬럼의 불필요한 접두어 제거 (http://, https://)
processed_data = df.withColumn("url", regexp_replace(col("url"), r"^https?://(?:[0-9]{1,3}\.){3}[0-9]{1,3}(:\d+)?/", ""))

#processed_data.show(n=df.count(), truncate=False)
processed_data.show(truncate=False)

# 데이터프레임의 행 수 출력
print("****" * 54)
print(processed_data.count())  # 행 수 출력

# 파티셔닝된 데이터를 저장할 S3 경로 설정
partitioned_output_path = "s3a://tripcok/preprocessed_data/step1"

# 데이터를 S3에 파티셔닝하여 저장
try:
    processed_data.write.partitionBy("url", "method").mode("overwrite").parquet(partitioned_output_path)
    print(f"Data successfully partitioned and saved to: {partitioned_output_path}")
except AnalysisException as e:
    print(f"Error: {e}")  # 예외 처리
finally:
    spark.stop()  # 작업 완료 후 Spark 세션 종료
