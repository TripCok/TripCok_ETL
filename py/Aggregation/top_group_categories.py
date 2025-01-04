import argparse
import logging
import os
import sys

import boto3
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, Window

# 프로젝트 루트 경로를 추가
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(project_root)

from py.common.SparkSess import SessionGen
from py.common.etl_utils import check_s3_folder_exists

logging.basicConfig(level=logging.INFO)

"""
생성된 그룹에서 가장 많이 선택된 카테고리 집계
"""


class TopGroupCategories:
    def __init__(self, process_date: str):
        # 스파크 세션 생성
        self.spark = SessionGen().create_session(app_name="TopGroupCategories", local=True)

        # S3 클라이언트 초기화
        self.s3 = boto3.client('s3')

        # 버킷 이름
        self.bucket_name = 'tripcok'

        # Parquet 탐색 폴더
        self.folder_path = "dm/cleansing_data"
        self.search_url = "_api_v1_group"
        self.search_method = "POST"
        self.out_path = "dm/top_group_categories"

        # 데이터를 처리하는 일자
        self.cur_date = process_date

        # 파티셔닝 작업을 할 컬럼
        self.partition_cols = ["cre_dtm"]

    def read(self):
        read_path = f"s3a://{self.bucket_name}/{self.folder_path}"
        logging.info(f"읽기 작업 시작 : {read_path}")

        read_df = self.spark.read.parquet(read_path).filter(
            f"cre_dtm='{self.cur_date}' AND url_part='{self.search_url}' AND method='{self.search_method}'"
        )

        return read_df

    def process(self, df: DataFrame):
        # response 스키마 정의
        response_schema = T.StructType([
            T.StructField("categories", T.ArrayType(
                T.StructType([
                    T.StructField("id", T.IntegerType(), True),
                    T.StructField("name", T.StringType(), True),
                    T.StructField("depth", T.IntegerType(), True)
                ])
            ), True)
        ])

        # response 컬럼 파싱
        parsed_df = df.withColumn("parsed_response", F.from_json(F.col("response"), response_schema))

        # categories 컬럼 추출
        parsed_df = parsed_df.withColumn("categories", F.col("parsed_response.categories"))

        # 카테고리 explode
        df_exploded = parsed_df.withColumn("category", F.explode(F.col("categories")))

        # 카테고리 ID와 이름 추출, etl_dtm 및 cre_dtm 유지
        df_exploded = df_exploded.select(
            F.col("traceId"),
            F.col("category.id").alias("category_id"),
            F.col("category.name").alias("category_name"),
            F.col("etl_dtm"),
            F.col("cre_dtm")
        )

        # 카테고리별 count
        category_count = df_exploded.groupBy("category_id", "category_name").agg(
            F.count("*").alias("count"),
            F.first("etl_dtm").alias("etl_dtm"),
            F.first("cre_dtm").alias("cre_dtm")
        )
        category_count = category_count.orderBy(F.desc("count"))

        return category_count

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
        new_df = new_df.select("category_id", "category_name", "count", "etl_dtm", "cre_dtm")

        union_df = origin_df.union(new_df)

        grouping_df = Window.partitionBy("cre_dtm").orderBy(F.desc("etl_dtm"))
        deduplicate_df = union_df.withColumn('row_no', F.row_number().over(grouping_df)) \
            .filter(F.col("row_no") == 1).drop('row_no')
        return deduplicate_df


def parse_arguments():
    parser = argparse.ArgumentParser(description="Top Group Categories Pipeline")
    parser.add_argument("--date", required=True, help="Execution date (YYYY-MM-DD)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    tgc = TopGroupCategories(args.date)
    read_df = tgc.read()
    process_df = tgc.process(read_df)
    tgc.write(process_df)
