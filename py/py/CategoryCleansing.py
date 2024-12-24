import argparse
import logging
from datetime import datetime, timedelta, timezone
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

# 로그 설정
logging.basicConfig(level=logging.INFO)

class CategoryCleansing:
    def __init__(self, bucket_name, folder_path, execute_date):
        self.s3 = boto3.client('s3')
        self.bucket_name = bucket_name
        self.execute_date = execute_date 
        self.folder_path = f"s3a://{self.bucket_name}/processed_data/{self.execute_date}/L/url=%2Fapi%2Fv1%2Fplace%2Fcategory%2Fall"
        self.output_path = f"s3a://{self.bucket_name}/processed_data/{self.execute_date}/L/url=%2Fapi%2Fv1%2Fplace%2Fcategory%2Falldd"

        # Spark 세션 생성
        self.spark = SparkSession.builder \
            .appName("Category Cleansing") \
            .getOrCreate()

    def get_schema(self):
        pass

    def load(self):
        file = self.spark.read.parquet(self.folder_path)
        return file

    def process_file(self):

        df = self.load()   

        # 데이터 확인 (옵션)
        df.show(truncate=False)

    def run(self):

        self.process_file()

        # Spark 세션 종료
        self.spark.stop()


# 실행 예제
if __name__ == "__main__":
    # KST 시간대 정의
    KST = timezone(timedelta(hours=9))

    # 현재 KST 날짜 가져오기
    current_date = datetime.now(KST).strftime('%Y%m%d')
    
    # 명령줄 인자 처리 
    parser = argparse.ArgumentParser(description="Logs Cleansing Pipeline")
    parser.add_argument("--bucket", required=False, default="tripcok", help="S3 bucket name")
    parser.add_argument("--folder", required=True, default=f"processed_data/", help="S3 folder path (prefix)")
    parser.add_argument("--date", required=True, help="Execution date (YYYYMMDD)")
    
    # parser 저장
    args = parser.parse_args()

    app = CategoryCleansing(bucket_name=args.bucket, folder_path=args.folder, execute_date=args.date)
    app.run()
