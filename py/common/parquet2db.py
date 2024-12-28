import argparse
import logging
from datetime import datetime, timedelta, timezone
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import col
from pyspark.sql import functions as F
import pymysql

# 로그 설정
logging.basicConfig(level=logging.INFO)



class InsertDb:
    def __init__(self, bucket_name, folder_path, execute_date):
        self.s3 = boto3.client('s3')
        self.bucket_name = bucket_name
        self.execute_date = execute_date 
        self.folder_path = f"s3a://{self.bucket_name}/processed_data/{self.execute_date}/T/url=%2Fapi%2Fv1%2Fapplication/method=PUT/"

        self.spark = SparkSession.builder \
            .appName("Category Cleansing") \
            .config("spark.driver.extraClassPath", "/home/ubuntu/spark/jars/postgresql-42.7.2.jar") \
            .config("spark.executor.extraClassPath", "/home/ubuntu/spark/jars/postgresql-42.7.2.jar") \
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
        df.printSchema()
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://13.209.89.20:5432/tripcok_db") \
            .option("dbtable", "tripcok_db.requests") \
            .option("user", "postgres") \
            .option("password", "tripcok1234") \
            .option("createTableColumnTypes", "requestTime VARCHAR, memberId INTEGER, groupAdminId INTEGER, applicationId INTEGER") \
            .mode("append") \
            .save()

        jdbcDF = self.spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://13.209.89.20:5432/tripcok_db") \
            .option("dbtable", "tripcok_db.requests") \
            .option("user", "postgres") \
            .option("password", "tripcok1234") \
            .load()

        jdbcDF.show()

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
    parser.add_argument("--folder", required=False, default=f"processed_data/", help="S3 folder path (prefix)")
    parser.add_argument("--date", required=True, help="Execution date (YYYYMMDD)")
    
    # parser 저장
    args = parser.parse_args()

    app = InsertDb(bucket_name=args.bucket, folder_path=args.folder, execute_date=args.date)
    app.run()
