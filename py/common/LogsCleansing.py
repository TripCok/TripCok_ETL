import argparse
import logging
import re  # re 모듈 추가
from datetime import datetime, timedelta, timezone
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import udf, col, lit, concat, regexp_replace

# 로그 설정
logging.basicConfig(level=logging.INFO)

class LogsCleansing:
    def __init__(self, bucket_name, folder_path, execute_date):
        # S3 클라이언트 초기화
        self.s3 = boto3.client('s3')
        
        # S3 버킷과 폴더 경로 설정
        self.bucket_name = bucket_name
        self.folder_path = f"{folder_path}/{execute_date}/"
        
        # 처리 날짜와 결과 경로 설정
        self.execute_date = execute_date
        self.output_path = f"s3a://{self.bucket_name}/processed_data/{self.execute_date}/L/"

        # Spark 세션 생성
        self.spark = SparkSession.builder \
            .appName("Logs Cleansing") \
            .getOrCreate()

    def get_schema(self):
        # JSON 데이터의 스키마 정의
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

    # UDF 함수 정의
    @staticmethod
    @udf(StructType([
        StructField("url_part", StringType(), True),
        StructField("last_token", StringType(), True)
    ]))
    def classify_url_by_last_token_udf(url):
        # URL에서 마지막 토큰을 추출
        last_token = url.rstrip("/").split("/")[-1]

        # 마지막 토큰이 숫자일 경우
        if re.fullmatch(r"\d+", last_token):
            # 숫자인 경우, 마지막 토큰을 포함한 앞부분 URL 반환
            return "/".join(url.rstrip("/").split("/")[:-1]) + "/", last_token
        else:
            # 숫자가 아닌 경우, 문자열을 포함한 전체 URL 반환
            return url, None

    def load_files(self):
        # 지정된 폴더에서 모든 객체 리스트 가져오기 (S3에서 파일 목록을 조회)
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=self.folder_path)

        # 파일이 존재하지 않으면 경고 메시지 출력
        if 'Contents' not in response:
            logging.warning(f"No files found in bucket '{self.bucket_name}' with prefix '{self.folder_path}'")
            return []

        # JSON 파일 목록만 필터링하여 반환
        files = [f"s3a://{self.bucket_name}/{obj['Key']}" for obj in response['Contents'] if obj['Key'].endswith('.json')]
        
        # JSON 파일이 없다면 경고 메시지 출력
        if not files:
            logging.warning(f"No JSON files found in bucket '{self.bucket_name}' with prefix '{self.folder_path}'")
        return files

    def process_file(self, path, schema):
        logging.info(f"Processing file: {path}")

        # JSON 파일을 읽어 DataFrame 생성
        df = self.spark.read.json(path, schema=schema)

        # 데이터 확인 (옵션) - DataFrame의 일부 출력
        df.show(truncate=False)

        # URL에서 'http://' 제거
        df = df.withColumn("url", regexp_replace(col("url"), r"^http://[^/]+", ""))

        df.show(truncate=False)

        # URL을 처리하여 새로운 컬럼을 생성
        df = df.withColumn("url_part", self.classify_url_by_last_token_udf(col("url")).getItem("url_part")) \
               .withColumn("pathParam", self.classify_url_by_last_token_udf(col("url")).getItem("last_token"))

        # URL과 method를 결합하여 새로운 컬럼 'url_method' 생성 (사이에 '/' 추가)
        df = df.withColumn("url_method", concat(col("url_part"), lit("/"), col("method")))

        df.show(truncate=False)

        # Parquet 형식으로 저장 (url_method로 파티셔닝, 5개의 파티션)
        df.coalesce(5).write.mode("overwrite").partitionBy("url_method").parquet(self.output_path)

    def run(self):
        # 로그에 처리 시작 시간 기록
        logging.info(f"[ {datetime.now()} ] AWS S3 {self.execute_date}일자 클렌징 시작")

        # 스키마 가져오기
        schema = self.get_schema()

        # 파일 로드
        files = self.load_files()

        # 각 파일 처리
        for path in files:
            print("****************")
            print(path)
            self.process_file(path, schema)

        # 처리 완료 로그
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

    # 명령줄 인자 처리
    args = parse_arguments()

    # LogsCleansing 객체 생성 후 실행
    app = LogsCleansing(bucket_name=args.bucket, folder_path=args.folder, execute_date=args.date)
    app.run()
