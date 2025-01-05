from fastapi import FastAPI, HTTPException
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
from common.parquet2db import S3
import logging

# FastAPI 초기화
app = FastAPI()

# SparkSession 초기화
DRIVER_CLASSPATH = os.getenv("MARIA_DRIVER_CLASSPATH")
EXECUTOR_CLASSPATH = os.getenv("MARIA_EXECUTOR_CLASSPATH")

spark = SparkSession.builder.appName("mariadb") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.driver.extraClassPath", DRIVER_CLASSPATH) \
    .config("spark.executor.extraClassPath", EXECUTOR_CLASSPATH) \
    .getOrCreate()

# S3 Helper Class
class S3:
    def __init__(self, spark_session):
        self.spark = spark_session

    def read(self, s3_base_path: str, partitioned_column: str, date: str):
        s3_path = f"{s3_base_path}/{partitioned_column}={date}/"
        try:
            df = self.spark.read.parquet(s3_path)
            return df
        except Exception as e:
            raise Exception(f"Error reading S3 data: {e}")

# S3 인스턴스 생성
s3 = S3(spark)

@app.get("/recommend")
async def get_recommendations(memberId: int):
    """
    API endpoint to get recommendations for a member.
    """
    s3_base_path = "s3a://tripcok/processed_data"
    partitioned_column = "cre_dtm"
    date = "2025-01-03"

    try:
        # S3 데이터 읽기
        df = s3.read(s3_base_path, partitioned_column, date)
        df.show()
        # memberId 기준으로 필터링
        filtered_df = df.filter(col("memberId") == memberId)
        filtered_df.show()
        # 필요한 컬럼 선택
        result_df = filtered_df.select("cid", "score")
        result_df.show(n=100, truncate=False)

        # 결과를 딕셔너리로 변환
        result = [row.asDict() for row in result_df.collect()]

        if filtered_df.isEmpty():
            print("진입")
            df.show()
            most_recommend_df = df.groupby('ml_mapping').reset_index(name='count').sort_values(by='count', ascending=False).head(5)
            result = [row.asDict() for row in most_recommend_df.collect()]
            return {"memberId": memberId, "recommendations": result}

        return {"memberId": memberId, "recommendations": result}

    except Exception as e:
        logging.error(f"Error occurred: {e}")
        raise HTTPException(status_code=500, detail=str(e))
