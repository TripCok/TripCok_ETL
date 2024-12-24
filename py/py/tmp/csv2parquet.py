from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, LongType

# SparkSession 생성
spark = SparkSession.builder.appName("Extract JSON from CSV").getOrCreate()

# S3에서 CSV 파일 로드
csv_file_path = "s3a://tripcok/preprocessed_data/2024-12-20/step0/part-00000-cd8d03aa-4003-41eb-964a-97b649259031-c000.csv"
csv_data = spark.read.option("mode", "DROPMALFORMED").option("header", "true").csv(csv_file_path)

# JSON 문자열 필드에 있는 이스케이프 문자 수정
#cleaned_data = csv_data.withColumn("json_str", regexp_replace(col("json_str"), '\\"', '"'))
#@print("cleaned*******************************************")
#cleaned_data.show(truncate=False)

csv_data.show(truncate=False)
cleaned_data = csv_data

# JSON 필드 스키마 정의
json_schema = StructType([
    StructField("traceId", StringType(), True),
    StructField("memberId", StringType(), True),
    StructField("clientIp", StringType(), True),
    StructField("url", StringType(), True),
    StructField("method", StringType(), True),
    StructField("request", StringType(), True),
    StructField("response", StringType(), True),
    StructField("statusCode", StringType(), True),
    StructField("time", LongType(), True)
])

# JSON 파싱
parsed_json = cleaned_data.withColumn("parsed", from_json(col("json_str"), json_schema))

parsed_json.select("parsed").show(truncate=False)

# 필요한 데이터 추출
extracted_data = parsed_json.select(
    col("parsed.traceId"),
    col("parsed.memberId"),
    col("parsed.clientIp"),
    col("parsed.url"),
    col("parsed.method"),
    col("parsed.request"),
    col("parsed.response"),
    col("parsed.statusCode"),
    col("parsed.time")
)

# 결과 확인
extracted_data.show(truncate=False)

# 추출한 데이터를 CSV로 저장
output_csv_path = "s3a://tripcok/preprocessed_data/2024-12-20/step1/extracted_data.csv"
extracted_data.write.mode("overwrite").option("header", "true").csv(output_csv_path)

print(f"Extracted data saved to {output_csv_path}")
