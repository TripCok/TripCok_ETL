import argparse
import logging
from datetime import datetime, timedelta, timezone
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import col, regexp_replace
# 로그 설정
logging.basicConfig(level=logging.INFO)

class LogsCleansing:
    def __init__(self, bucket_name, folder_path, execute_date):
        self.s3 = boto3.client('s3')
        self.bucket_name = bucket_name
        self.folder_path = f"{folder_path}/{execute_date}/"
        self.execute_date = execute_date
        self.output_path = f"s3a://{self.bucket_name}/processed_data/{self.execute_date}/L/"

        # Spark 세션 생성
        self.spark = SparkSession.builder \
            .appName("Logs Cleansing") \
            .getOrCreate()

    def get_schema(self):
        # JSON 데이터의 외부 스키마 정의
        schema = StructType([
            StructField("traceId", StringType(), True),
            StructField("memberId", StringType(), True),
            StructField("clientIp", StringType(), True),
            StructField("url", StringType(), True),
            StructField("method", StringType(), True),
            StructField("requestParam", StringType(), True),
            StructField("request", StringType(), True),
            StructField("response", StringType(), True),
            StructField("statusCode", StringType(), True),
            StructField("requestTime", StringType(), True),
            StructField("time", LongType(), True),
        ])
        return schema
    @udf(StringType())
    def classify_url_by_last_token_udf(url):
        last_token = url.rstrip("/").split("/")[-1]
        if re.fullmatch(r"\d+", last_token):
            return last_token  # 숫자로 끝나면 마지막 토큰 반환
        else:
            return None  # 숫자가 아니면 None 반환

    def load_files(self):
        # 지정된 prefix에서 모든 객체 리스트 가져오기
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=self.folder_path)
        
        if 'Contents' not in response:
            logging.warning(f"No files found in bucket '{self.bucket_name}' with prefix '{self.folder_path}'")
            return []

        files = [f"s3a://{self.bucket_name}/{obj['Key']}" for obj in response['Contents'] if obj['Key'].endswith('.json')]
        if not files:
            logging.warning(f"No JSON files found in bucket '{self.bucket_name}' with prefix '{self.folder_path}'")
        return files

    def process_file(self, path, schema):
        logging.info(f"Processing file: {path}")

        # DataFrame 생성
        df = self.spark.read.json(path, schema=schema)

        # 데이터 확인 (옵션)
        df.show(truncate=False)
        # pathParam 이라는 칼럼 생성
        df = df.withColumn("url", regexp_replace(col("url"), r"^http://[^/]+", ""))
        
        # pathParam 컬럼 생성
        df = df.withColumn("pathParam", classify_url_by_last_token_udf(col("url")))
        
        # Parquet 형식으로 저장 (url 및 memberId로 파티셔닝, 5개의 파티션)
        df.coalesce(5).write.mode("append").partitionBy("url").parquet(self.output_path)

    def run(self):
        logging.info(f"[ {datetime.now()} ] AWS S3 {self.execute_date}일자 클렌징 시작")

        schema = self.get_schema()
        files = self.load_files()
        for path in files:
            print("****************")
            print(path)
            self.process_file(path, schema)

        logging.info(f"모든 파일 처리가 완료되었습니다. 결과가 {self.output_path}에 저장되었습니다.")

        # Spark 세션 종료
        self.spark.stop()
    

def parse_arguments():
    """
    명령줄 인자를 처리하는 함수
    """
    parser = argparse.ArgumentParser(description="Logs Cleansing Pipeline")
    parser.add_argument("--bucket", required=False, default="tripcok", help="S3 bucket name")
    parser.add_argument("--folder", required=True, default=f"processed_data/", help="S3 folder path (prefix)")
    parser.add_argument("--date", required=True, help="Execution date (YYYY-MM-DD)")
    return parser.parse_args()


# 실행 예제
if __name__ == "__main__":
    
    # parser 저장
    args = parse_arguments()

    app = LogsCleansing(bucket_name=args.bucket, folder_path=args.folder, execute_date=args.date)
    app.run()
