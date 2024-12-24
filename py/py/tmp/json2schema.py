from pyspark.sql import SparkSession
import json

# SparkSession 생성
spark = SparkSession.builder \
    .appName("Infer JSON Schema") \
    .getOrCreate()

# JSON 데이터 파일 경로 (또는 JSON 데이터 읽기)
json_file_path = "path/to/your/json_file.json"

# JSON 데이터 읽기
df = spark.read.json(json_file_path)

# 스키마 추출
schema_json = df.schema.json()

# JSON 문자열을 Python 딕셔너리로 변환
schema_dict = json.loads(schema_json)

# 결과 출력
print("Schema as Python dictionary:")
print(json.dumps(schema_dict, indent=4))
