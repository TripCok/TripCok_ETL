import glob
import logging
import os
import sys

from botocore.response import get_response
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType, MapType
from pyspark.sql.functions import from_json, col
from common_use.SparkSess import SessionGen

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))

class MemberPlaceRecommend():

    def __init__(self, excute_date):
        #self.run_env = "local"
        self.excute_date = excute_date
        self.s3_path = f"s3a://tripcok/dm/cleansing_data/cre_dtm={excute_date}/url_part=_api_v1_place_/method=GET/"
        self.spark = SessionGen().create_session(app_name="member_place_recommend", local=False)

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
        # Parquet 데이터 로드
        ml_map_df = self.spark.read.option("header", "true").csv("./place.csv")
        df = self.spark.read.parquet(self.s3_path)

        df = df.withColumn("parsed_response", from_json(col("response"),self.get_schema()))

        # parsed_response에서 데이터 추출
        df = (df.select(
            col("*"),  # 기존 모든 칼럼
            col("parsed_response.id").alias("id"),
            col("parsed_response.name").alias("name"),
            col("parsed_response.categoryNames").alias("categoryNames")
        ).drop("parsed_response").drop("response").drop("clientIp").drop("url")
              .drop("requestParam").drop("request").drop("statusCode").drop("time"))

        df.show(n=10, truncate=False)

        ml_map_df = ml_map_df.filter(col("id").cast("int").isNotNull())
        ml_map_df = (ml_map_df.select(
            col("id").alias("id"),
            #col("name").alias("name"),
            col("ml_mapping_id").alias("ml_mapping_id"),
            #col("address").alias("address"),
        ).drop("latitude").drop("longitude").drop("time").drop("create_time").drop("update_time"))

        ml_map_df.show(n=10, truncate=False)

        ml_map_df.createOrReplaceTempView("ml_map_table")
        df.createOrReplaceTempView("df_table")

        query ="""
            SELECT * FROM df_table INNER JOIN ml_map_table ON df_table.id = ml_map_table.id
        """

        joined_df = self.spark.sql(query)

        joined_df.show(n=10, truncate=False)

        return joined_df

    def process(self, df) -> DataFrame:
        return df

    def write(self) -> None:
        return None
    def _deduplicate(self) -> None:
        return None
    def run(self):
        df = self.load()
        df.show(n =10,truncate=False)
        process_df, ml_df = self.process(df)
        process_df.show(n=1, truncate=False)


if __name__ == "__main__":
    try:
        # 실행 인자 설정
        execute_date = "2024-12-28"  # 실제 실행 시 argparse 등으로 동적으로 설정 가능
        etl_job = MemberPlaceRecommend(execute_date)
        etl_job.run()
        logger.info('ETL Job Completed Successfully')
        sys.exit(0)
    except Exception as e:
        logger.error(f"ETL Job Failed: {e}")
        sys.exit(1)