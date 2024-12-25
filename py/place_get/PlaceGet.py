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

class PlaceGet:
    def __init__(self, bucket_name, folder_path, execute_date):
        self.s3 = boto3.client('s3')
        self.bucket_name = bucket_name
        self.execute_date = execute_date 
        self.folder_path = f"s3a://{self.bucket_name}/processed_data/{self.execute_date}/L/url=%2Fapi%2Fv1%2Fapplication/method=PUT/"
        self.output_path = f"s3a://{self.bucket_name}/processed_data/{self.execute_date}/T/url=%2Fapi%2Fv1%2Fapplication/method=PUT/"

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
        
        # requestTime, groupAdminId, memberId, applicationId 추출 
       
        from pyspark.sql import functions as F

        #데이터프레임 컬럼 이름 변경 및 큰따옴표 제거
        result_df = df.select(
            F.col("requestTime").alias("requesttime"),
            F.col("memberId").alias("memberid"),
            F.col("request").alias("request")
        ).withColumn(
            "requesttime", F.col("requesttime").cast("timestamp")
        ).withColumn(
            "memberid", F.col("memberid").cast("int")
        ).withColumn(
            "groupadminid", F.col("request").substr(17, 3).cast("int")
        ).withColumn(
            "applicationid", F.col("request").substr(40, 3).cast("int")
        ).drop("request")
        #.drop("memberId").drop("groupAdminId").drop("appllicationId").drop("requestTime")

        result_df.show()
        print(result_df.schema)
        
        # Null 값이 포함된 행 제거
        result_df = result_df.na.drop()
        
        #parquet로 저장
        result_df.coalesce(5).write.mode("overwrite").parquet(self.output_path)    

    def run(self): 
        self.process_file()
        
        # Spark 세션 종료
        self.spark.stop()

    def parse_arguments():
        """
        명령줄 인자를 처리하는 함수
        """
        parser = argparse.ArgumentParser(description="Logs Cleansing Pipeline")
        parser.add_argument("--bucket", required=False, default="tripcok", help="S3 bucket name")
        parser.add_argument("--folder", required=True, default=f"processed_data/", help="S3 folder path (prefix)")
        parser.add_argument("--date", required=True, help="Execution date (YYYYMMDD)")
        return parser.parse_args()

# 실행 예제
if __name__ == "__main__":
    
    args = parser.arguments()

    app = ApplicationCleansingPut(bucket_name=args.bucket, folder_path=args.folder, execute_date=args.date)

    app.run()
