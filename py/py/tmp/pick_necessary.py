from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, regexp_replace, when, udf
from pyspark.sql.types import BooleanType
from datetime import datetime, timedelta, timezone

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("Extract JSON from Logs") \
    .getOrCreate()

# S3 경로 설정
s3_path = "s3a://tripcok/topics/logs/2024/12/19/logs+0+0000000000.json"

# 1. 텍스트 파일로 읽기
raw_text = spark.read.text(s3_path)

# 결과 확인
print("**** Raw Text ****")
raw_text.show(truncate=False)

# 2. 초기화: cleaned_value 컬럼 추가
clean_text = raw_text.withColumn("cleaned_value", col("value"))

# 3. HTML 특수문자 처리
clean_text = clean_text.withColumn(
    "cleaned_value",
    regexp_replace(col("cleaned_value"), r'\\n', '\n')  # \n을 실제 줄바꿈으로 변환
)

# 4. '/admin' URL 포함 행 삭제
clean_text = clean_text.filter(~col("cleaned_value").like('%/admin%'))

# 5. JSON 문자열 추출
json_pattern = r"\{.*\}"  # { }로 감싸진 JSON 문자열만 추출
extracted_json = clean_text.withColumn("json_str", regexp_extract(col("cleaned_value"), json_pattern, 0))

# 6. response 필드가 HTML 콘텐츠를 포함하거나 비어있는 경우 해당 행 삭제
def filter_html_response(value):
    # response가 NULL이거나 HTML 콘텐츠를 포함한 경우 False 반환
    if value is None or value.startswith('<!DOCTYPE html>'):
        return False
    return True

# response 필드 필터링 (UDF 사용) => BooleanType 사용
filter_html_response_udf = udf(filter_html_response, BooleanType())
filtered_json = extracted_json.filter(filter_html_response_udf(col("json_str")))

# 7. json_str만 추출
filtered_json_str = filtered_json.select("json_str")

# 필터링된 결과 확인
print("**** Filtered JSON (Only json_str) ****")
filtered_json_str.show(truncate=False)

# 8. 현재 KST 날짜 설정
KST = timezone(timedelta(hours=9))
current_date = datetime.now(KST).strftime('%Y-%m-%d')
print(current_date)

# 저장할 S3 경로에 현재 날짜 포함
partitioned_output_path = f"s3a://tripcok/preprocessed_data/{current_date}/step0"

# 9. JSON 형식으로 S3에 저장
filtered_json_str.write.mode("overwrite").option("header", "true").csv(partitioned_output_path)

