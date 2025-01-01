import argparse
import logging

import boto3
from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F
import pyspark.sql.types as T
from py.common.SparkSess import SessionGen
from py.common.etl_utils import check_s3_folder_exists

logging.basicConfig(level=logging.INFO)

"""
해당 일자에 생성된 모임 수 구하기
"""


class CreatedGroupCounting:

    def __init__(self, process_date):
        # 스파크 세션 생성
        self.spark = SessionGen().create_session(app_name="AggCreateGroup", local=True)

        # S3 클라이언트 초기화
        self.s3 = boto3.client('s3')

        # 버킷 이름
        self.bucket_name = 'tripcok'

        # Parquet 탐색 폴더
        self.folder_path = "dm/cleansing_data"
        self.search_url = "_api_v1_group"
        self.search_method = "POST"
        self.out_path = "dm/group_counting"

        # 데이터를 처리하는 일자
        self.cur_date = process_date

        # 파티셔닝 작업을 할 컬럼
        self.partition_cols = ["cre_dtm"]

    # 1. 읽기
    def read(self):
        read_path = f"s3a://{self.bucket_name}/{self.folder_path}"
        logging.info(f"읽기 작업 시작 : {read_path}")

        read_df = self.spark.read.parquet(read_path).filter(
            f"cre_dtm='{self.cur_date}' AND url_part='{self.search_url}' AND method='{self.search_method}'"
        )

        return read_df

    # 2. 가공
    def process(self, df: DataFrame):
        # 201 Created Http 통신으로 성공적으로 모임이 생성된 로그만 필터링
        filter_df = df.filter("statusCode = 201")

        # ETL 동작 시간 업데이트
        update_df = filter_df.withColumn("etl_dtm", F.current_timestamp())

        counting_df = update_df.groupBy("cre_dtm").agg(
            F.first("etl_dtm").alias("etl_dtm"),
            F.count("*").alias("count")
        )

        return counting_df

    # 3. 쓰기
    def write(self, df: DataFrame):
        write_path = f"s3a://{self.bucket_name}/{self.out_path}/"

        # 기존 Parquet 검사
        if check_s3_folder_exists(self.bucket_name, self.out_path):
            logging.info(f"S3 폴더가 이미 존재 합니다. : {write_path}")
            df = self.deduplicate(df)

        df = df.withColumn("etl_dtm", F.col("etl_dtm").cast("timestamp"))
        df.coalesce(5).write.mode("overwrite").partitionBy(self.partition_cols).parquet(write_path)

    def deduplicate(self, new_df):
        origin_read_path = f"s3a://{self.bucket_name}/{self.out_path}/"
        origin_df = self.spark.read.parquet(origin_read_path)

        origin_df = origin_df.withColumn("etl_dtm", F.col("etl_dtm").cast("timestamp"))
        new_df = new_df.select("etl_dtm", "count", "cre_dtm")

        union_df = origin_df.union(new_df)
        grouping_df = Window.partitionBy("cre_dtm").orderBy(F.desc("etl_dtm"))
        deduplicate_df = union_df.withColumn('row_no', F.row_number().over(grouping_df)) \
            .filter(F.col("row_no") == 1).drop('row_no')
        return deduplicate_df


def parse_arguments():
    parser = argparse.ArgumentParser(description="Top Application Groups Pipeline")
    parser.add_argument("--date", required=True, help="Execution date (YYYY-MM-DD)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    cgc = CreatedGroupCounting(args.date)
    read_df = cgc.read()
    process_df = cgc.process(read_df)
    cgc.write(process_df)
