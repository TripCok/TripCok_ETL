import glob
import logging
import os
import sys

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from botocore.response import get_response
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, StringType, DoubleType, LongType, ArrayType, MapType
from pyspark.sql.functions import from_json, col, udf, count, explode
from common_use.SparkSess import SessionGen
from common_use.ModelServer import ModelServer

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))

class MemberPlaceRecommend():


    def __init__(self, execute_date):
        self.execute_date = execute_date
        self.s3_path = f"s3a://tripcok/dm/cleansing_data/cre_dtm={execute_date}/url_part=_api_v1_place_/method=GET/"
        self.spark = SessionGen().create_session(app_name="member_place_recommend", local=False)

        # UDF 등록
        self.recommendations_udf = udf(
            MemberPlaceRecommend.get_recommendations,
            StructType([
                StructField("cids", ArrayType(StringType()), True),
                StructField("scores", ArrayType(FloatType()), True)
            ])
        )

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
            #col("parsed_response.name").alias("name"),
            #col("parsed_response.categoryNames").alias("categoryNames")
        ).drop("parsed_response").drop("response").drop("clientIp").drop("url")
        .drop("requestParam").drop("request").drop("statusCode").drop("time"))

        #df.show(n=10, truncate=False)

        ml_map_df = ml_map_df.filter(col("id").cast("int").isNotNull())
        ml_map_df = (ml_map_df.select(
            col("id").alias("id"),
            #col("name").alias("name"),
            col("ml_mapping_id").alias("ml_mapping_id"),
            #col("address").alias("address"),
        ).drop("latitude").drop("longitude").drop("time").drop("create_time").drop("update_time"))

        #ml_map_df.show(n=10, truncate=False)

        joined_df = df.join(ml_map_df, on="id", how="inner")

        #joined_df.show(n=10, truncate=False)
        joined_df.createOrReplaceTempView("joined_df")

        # 윈도우 정의
        window_spec = Window.partitionBy("memberId").orderBy(col("counts").desc())

        # counts 계산
        grouped_df = joined_df.groupBy("memberId", "ml_mapping_id").agg(count("*").alias("counts"))

        # 순위 계산
        ranked_df = grouped_df.withColumn("rank", row_number().over(window_spec))

        # rank가 1인 행만 필터링
        top_counts_df = ranked_df.filter(col("rank") == 1).select("memberId", "ml_mapping_id", "counts")

        top_counts_df.show()

        return top_counts_df

    @staticmethod
    def get_recommendations(ml_mapping):
        # 작업자에서 직접 ModelServer 객체 생성
        model_server = ModelServer()

        # FastAPI 요청
        body = {"contentids": [ml_mapping], "top_k": 5}
        cids, scores = model_server.request2server(api="/recommend/", param=2750144, body=body, test=True)
        if cids and scores:
            return {"cids": cids, "scores": scores}
        return {"cids": [], "scores": []}

    def process(self, df) -> DataFrame:
        df = df.withColumn(
            "recommends",
            self.recommendations_udf(col("ml_mapping_id"))
        )

        df = df.select(
            col("*"),
            col("recommends.cids").alias("cids"),
            col("recommends.scores").alias("scores")
        ).drop("recommends")

        df.show(n=10, truncate=False)

        # 배열 컬럼(cids, scores)을 개별 행으로 분리
        normalized_df = df.withColumn("cid", explode(col("cids"))) \
            .withColumn("score", explode(col("scores"))) \
            .drop("cids", "scores")

        # 결과 확인
        normalized_df.show(truncate=False)
        return normalized_df

    def write(self) -> None:
        return None
    def _deduplicate(self) -> None:
        return None
    def run(self):
        df = self.load()
        df.show(n =10,truncate=False)
        processed_df = self.process(df)


if __name__ == "__main__":
    try:
        # 실행 인자 설정
        execute_date = "2024-12-27"  # 실제 실행 시 argparse 등으로 동적으로 설정 가능
        etl_job = MemberPlaceRecommend(execute_date)
        etl_job.run()
        logger.info('ETL Job Completed Successfully')
        sys.exit(0)
    except Exception as e:
        logger.error(f"ETL Job Failed: {e}")
        sys.exit(1)