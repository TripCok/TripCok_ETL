import glob
import logging
import os
import sys
import boto3
import time
import asyncio
import argparse
import uuid
from datetime import datetime


from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from botocore.response import get_response
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, StringType, DoubleType, LongType, ArrayType, MapType
from pyspark.sql.functions import from_json, col, udf, count, explode, expr, map_keys, current_timestamp, to_date
from common.SparkSess import SessionGen
from common.ModelServer import ModelServer
from place_get.AsyncAPIClient import AsyncAPIClient
from requests.utils import extract_zipped_paths
from common.parquet2db import JDBC

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))

class MemberPlaceRecommend():

    def __init__(self, execute_date):
        self.execute_date = execute_date
        self.s3_path = f"s3a://tripcok/dm/cleansing_data/cre_dtm={execute_date}/url_part=_api_v1_place_/method=GET/"
        self.spark = SessionGen().create_session(app_name="member_place_recommend", local=False)
        self.output_path = f"s3a://tripcok/processed_data/"
        self.partition_cols = ["cre_dtm"]
        self.bucket_name = "tripcok"

    def get_schema(self):
        # JSON 스키마 정의
        response_schema = StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("description", StringType(), True),
            StructField("placeThumbnail", StringType(), True),
            StructField("address", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("startTime", StringType(), True),
            StructField("endTime", StringType(), True),
            StructField("createTime", StringType(), True),
            StructField("updateTime", StringType(), True),
            StructField("categoryNames", ArrayType(StringType()), True),
            StructField("images", ArrayType(StringType()), True)
        ])
        return response_schema

    def load(self):

        """
        place 관련 정보를 가진 ml_map_df 얻기
        """

        current_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(current_dir, "place.csv")

        ml_map_df = self.spark.read.option("header", "true").csv(file_path)
        df = self.spark.read.parquet(self.s3_path)

        df = df.withColumn("parsed_response", from_json(col("response"),self.get_schema()))

        df.show(truncate=False)

        df = df.withColumn("cre_dtm", to_date(col("requestTime"))) \
            .withColumn("etl_dtm", current_timestamp())

        print("*************************************")
        df.select("cre_dtm").show()
        # parsed_response에서 데이터 추출
        df = (df.select(
            col("*"),  # 기존 모든 칼럼
            col("parsed_response.id").alias("id"),
            col("cre_dtm"),
            col("etl_dtm"),
            #col("parsed_response.name").alias("name"),
            #col("parsed_response.categoryNames").alias("categoryNames")
        ).drop("parsed_response").drop("response").drop("clientIp").drop("url")
        .drop("requestParam").drop("request").drop("statusCode").drop("time"))

        #df.show(n=10, truncate=False)
        ml_map_df = ml_map_df.filter(col("id").cast("int").isNotNull())
        ml_map_df.show(n=10, truncate=False)

        ml_map_df = (ml_map_df.select(
            col("id").alias("id"),
            #col("placeName").alias("name"),
            col("ml_mapping_id").alias("ml_mapping_id"),
            #col("address").alias("address"),
        ).drop("latitude").drop("longitude").drop("time").drop("create_time").drop("update_time"))

        #ml_map_df.show(n=10, truncate=False)

        joined_df = df.join(ml_map_df, on="id", how="inner")
        joined_df.show(truncate=False)

        #joined_df.show()
        # 윈도우 정의
        window_spec = Window.partitionBy("memberId").orderBy(col("counts").desc())

        # counts 계산
        grouped_df = joined_df.groupBy("memberId", "ml_mapping_id","id").agg(count("*").alias("counts"))

        # 순위 계산
        ranked_df = grouped_df.withColumn("rank", row_number().over(window_spec))
        #ranked_df.printSchema()

        # rank가 1인 행만 필터링
        top_counts_df = ranked_df.filter(col("rank") == 1).select("memberId", "ml_mapping_id", "counts")

        top_counts_df.show()

        result_df = top_counts_df.join(joined_df.select("traceId", "memberId", "ml_mapping_id", "etl_dtm","cre_dtm","id"),
                                       on=["memberId", "ml_mapping_id"], how="left").orderBy("traceId")

        result_df = result_df.dropDuplicates(["memberId", "ml_mapping_id"])
        result_df.show(truncate=False)

        return result_df


    async def fetch_data_in_batches(self, asynchronicity, data):
        return await asynchronicity.get_recommendations(data)

    def fetch_and_save_results(self, asynchronicity, df):
        # 비동기 호출 후 저장 (사전에 API 호출 수행)
        data = df.select("memberId", "ml_mapping_id").distinct().collect()
        api_results = asyncio.get_event_loop().run_until_complete(
            self.fetch_data_in_batches(asynchronicity,[(row.memberId, row.ml_mapping_id) for row in data])
        )
        return api_results

    def process(self,asynchronicity, df) -> DataFrame:
        print("process 진입")
        api_results = self.fetch_and_save_results(asynchronicity, df)
        results_df = self.spark.createDataFrame(api_results)
        #results_df.write.parquet("./result.parquet")
        #results_df = self.spark.read.parquet("./result.parquet/")

        """
        FastAPI의 결과물을 반환 받아서 DataFrame 형태로 변환
        """
        results_df.show(truncate=False)
        exploded_df = results_df.select(col("memberId"),explode(col("recommendations")).alias("result_map"))
        normalized_df = exploded_df.select(col("memberId"),explode(col("result_map")).alias("cid", "score"))
        normalized_df = normalized_df.join(df, on="memberId", how="left_outer")
        normalized_df =normalized_df.orderBy("memberId","cid")
        normalized_df.show(n=100, truncate=False)
        """
        trace_id 고유화한 값으로 변경
        """
        window_spec = Window.partitionBy("traceId").orderBy(F.lit(0))  # orderBy에 적절한 기준 열이 필요하다면 변경
        normalized_df = normalized_df.withColumn("traceIdSuffix", F.row_number().over(window_spec))

        # trace_id_with_suffix 열 생성
        normalized_df = normalized_df.withColumn(
            "traceIdWithSuffix",
            F.concat_ws("_", F.col("traceId"), F.col("traceIdSuffix").cast("string"))
        )
        # 불필요한 열 삭제
        normalized_df = normalized_df.drop("traceIdSuffix").drop("traceId")
        # trace_id_with_suffix를 trace_id로 이름 변경
        normalized_df = normalized_df.withColumnRenamed("traceIdWithSuffix", "traceId")
        # 결과 출력
        normalized_df.show(truncate=False)

        """
        place 관련 정보 업데이트
        """
        current_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(current_dir, "place.csv")
        ml_map_df = self.spark.read.option("header", "true").csv(file_path)
        joined_df = normalized_df.join(ml_map_df, normalized_df.cid == ml_map_df.ml_mapping_id, how="left_outer")
        joined_df.show(truncate=False)
        """
        필요한 칼럼만 추출
        """
        joined_df = joined_df.select(
            col("cid").alias("cid"),
            col("memberId").alias("memberId"),
            col("score").alias("score"),
            col("traceId").alias("traceId"),
            col("etl_dtm").alias("etl_dtm"),
            col("cre_dtm").alias("cre_dtm"),
            col("name").alias("name"),
        ).drop("ml_mapping_id")
        print("joined_df")
        joined_df = joined_df.orderBy("memberId","cid")
        joined_df.show(n=100,truncate=False)
        joined_df.printSchema()

        return joined_df


    def check_s3_folder_exists(self):
        s3 = boto3.client('s3')
        response = s3.list_objects_v2(Bucket=self.bucket_name, Prefix=self.output_path, Delimiter='/')
        return 'Contents' in response or 'CommonPrefixes' in response

    def write(self, df):

        # # AWS 데이터가 있는지 없는지 검증
        if self.check_s3_folder_exists():
            self.deduplicate(df)

        # Parquet 형식으로 저장 (url_method로 파티셔닝, 5개의 파티션)
        df.coalesce(5).write.mode("append").partitionBy(self.partition_cols).parquet(self.output_path)

    def deduplicate(self, new_df):
        origin_df = self.spark.read.parquet(self.output_path)

        union_df = origin_df.union(new_df)
        grouping_df = Window.partitionBy("memberId").orderBy(col("etl_dtm").desc)
        deduplicate_df = union_df.withColumn('row_no', row_number().over(grouping_df)) \
            .filter(col("row_no") == 1).drop('row_no')
        return deduplicate_df

    def run(self, asynchronicity):
        df = self.load()
        processed_df = self.process(asynchronicity,df)
        self.write(processed_df)
        jdbc =JDBC(self.spark)

        if datetime.strptime(self.execute_date, "%Y-%m-%d") >= datetime(2025, 1, 3):
            print("storeDataForPeriod 진입")
            jdbc.storeDataForPeriod(self.output_path, "cre_dtm", self.execute_date,7, "memberplacerecommend")

def parse_arguments():
    """
    명령줄 인자를 처리하는 함수
    """
    parser = argparse.ArgumentParser(description="Logs Cleansing Pipeline")
    parser.add_argument("--date", required=True, help="Execution date (YYYY-MM-DD)")
    return parser.parse_args()

if __name__ == "__main__":
    try:
        # 실행 인자 설정
        args = parse_arguments()
        etl_job = MemberPlaceRecommend(args.date)
        ass = AsyncAPIClient()
        etl_job.run(ass)
        logger.info('ETL Job Completed Successfully')
        sys.exit(0)

    except Exception as e:
        logger.error(f"ETL Job Failed: {e}")
        sys.exit(1)