import argparse
import logging

import boto3
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, Window

from py.common.SparkSess import SessionGen
from py.common.etl_utils import check_s3_folder_exists

# 로그 설정
logging.basicConfig(level=logging.INFO)


class TopApplicationGroups:
    def __init__(self, process_date):
        # 스파크 세션 생성
        self.spark = SessionGen().create_session(app_name="TopApplicationGroups", local=True)

        # S3 클라이언트 초기화
        self.s3 = boto3.client('s3')

        # 버킷 이름
        self.bucket_name = 'tripcok'

        # Parquet 탐색 폴더
        self.folder_path = "dm/cleansing_data"

        self.search_url = "_api_v1_application"
        self.search_method = "POST"
        self.out_path = "dm/top_application_groups"

        self.partition_cols = ["cre_dtm"]
        self.cur_date = process_date

    def read(self):
        bucket_path = f"s3a://{self.bucket_name}/{self.folder_path}/"
        logging.info(f"읽기 작업 시작 : {bucket_path}")
        df = self.spark.read.parquet(bucket_path).filter(
            f"cre_dtm='{self.cur_date}' AND url_part='{self.search_url}' AND method='{self.search_method}'")

        return df

    def process(self, df: DataFrame):
        # Request 의 schema 정의
        request_schema = T.StructType([
            T.StructField("memberId", T.IntegerType(), True),
            T.StructField("groupId", T.IntegerType(), True)
        ])

        # parsed_request 라는 새로운 컬럼을 추가 해주고 etl_dtm 의 시간을 현재 시간으로 업데이트 해준다.
        parsed_df = df.withColumn("parsed_request", F.from_json(F.col('request'), request_schema)) \
            .withColumn("etl_dtm", F.current_timestamp())

        # 필요한 컬럼만 선택
        selected_df = parsed_df.select(
            F.col("parsed_request.memberId").alias("member_id"),
            F.col("parsed_request.groupId").alias("group_id"),
            F.col("etl_dtm"),
            F.col("cre_dtm")
        )

        aggregated_df = selected_df.groupBy("group_id").agg(
            F.count("*").alias("count"),
            F.first("etl_dtm").alias("etl_dtm"),
            F.first("cre_dtm").alias("cre_dtm")
        ).orderBy("count", ascending=False)

        return aggregated_df

    def write(self, df):
        logging.info("쓰기 작업 시작")
        if check_s3_folder_exists(self.bucket_name, self.out_path):
            logging.info(f"S3 폴더가 이미 존재 합니다. : {self.out_path}")
            df = self.deduplicate(df)

        write_path = f"s3a://{self.bucket_name}/{self.out_path}/"
        df.coalesce(5).write.mode("overwrite").partitionBy(self.partition_cols).parquet(write_path)

    def deduplicate(self, new_df):
        origin_read_path = f"s3a://{self.bucket_name}/{self.out_path}/"

        origin_df = self.spark.read.parquet(origin_read_path)

        union_df = origin_df.union(new_df)
        grouping_df = Window.partitionBy("group_id", "cre_dtm").orderBy(F.desc("etl_dtm"))
        deduplicate_df = union_df.withColumn('row_no', F.row_number().over(grouping_df)) \
            .filter(F.col("row_no") == 1).drop('row_no')
        return deduplicate_df


def parse_arguments():
    parser = argparse.ArgumentParser(description="Top Application Groups Pipeline")
    parser.add_argument("--date", required=True, help="Execution date (YYYY-MM-DD)")
    return parser.parse_args()


if __name__ == "__main__":
    # 명령줄 인자 처리
    args = parse_arguments()
    tag = TopApplicationGroups(args.date)
    read_df = tag.read()
    proccess = tag.process(read_df)
