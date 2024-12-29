import glob
import logging
import os
import sys
import boto3

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from botocore.response import get_response
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, StringType, DoubleType, LongType, ArrayType, MapType
from pyspark.sql.functions import from_json, col, udf, count, explode, expr
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
        self.output_path = f"s3a://tripcok/processed_data/"
        self.partition_cols = ["etl_dtm"]
        self.bucket_name = "tripcok"


        # UDF 등록
        self.recommendations_udf = udf(
            MemberPlaceRecommend.get_recommendations,
            ArrayType(
                MapType(StringType(), FloatType())
            )
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

        joined_df.printSchema()
        # 윈도우 정의
        window_spec = Window.partitionBy("memberId").orderBy(col("counts").desc())

        # counts 계산
        grouped_df = joined_df.groupBy("memberId", "ml_mapping_id").agg(count("*").alias("counts"))

        # 순위 계산
        ranked_df = grouped_df.withColumn("rank", row_number().over(window_spec))
        ranked_df.printSchema()

        # rank가 1인 행만 필터링
        top_counts_df = ranked_df.filter(col("rank") == 1).select("memberId", "ml_mapping_id", "counts")

        top_counts_df.show()

        result_df = top_counts_df.join(joined_df.select("traceId", "memberId", "ml_mapping_id", "etl_dtm"),
                                       on=["memberId", "ml_mapping_id"], how="left")

        result_df.show(truncate=False)

        return result_df

    @staticmethod
    def get_recommendations(ml_mapping):
        # 작업자에서 직접 ModelServer 객체 생성
        model_server = ModelServer()

        # FastAPI 요청
        body = {"contentids": [ml_mapping], "top_k": 5}
        response = model_server.request2server(api="/recommend/", param=2750144, body=body, test=True)
        if response:
            return response
        else:
            return []

    def process(self, df) -> DataFrame:
        df = df.withColumn(
            "recommends",
            self.recommendations_udf(col("ml_mapping_id"))
        )
        # recommends 배열을 개별 Map으로 분리
        exploded_df = df.withColumn("recommendation", explode(col("recommends")))

        exploded_df.printSchema()

        # Extract key and value using keys and values functions
        normalized_df = exploded_df.withColumn("cid", expr("map_keys(recommendation)[0]")) \
            .withColumn("score", expr("map_values(recommendation)[0]")).drop("recommends").drop("recommendation")
        # 결과 확인
        normalized_df.show(truncate=False)
        return normalized_df

    def check_s3_folder_exists(self):
        s3 = boto3.client('s3')
        response = s3.list_objects_v2(Bucket=self.bucket_name, Prefix=self.output_path, Delimiter='/')
        return 'Contents' in response or 'CommonPrefixes' in response

    def write(self, df):

        # 데이터 확인 (옵션) - DataFrame의 일부 출력
        df.show(truncate=False)

        # AWS 데이터가 있는지 없는지 검증
        if self.check_s3_folder_exists():
            self.deduplicate(df)

        # Parquet 형식으로 저장 (url_method로 파티셔닝, 5개의 파티션)
        df.coalesce(5).write.mode("append").partitionBy(self.partition_cols).parquet(self.output_path)

    def deduplicate(self, new_df):
        origin_df = self.spark.read.parquet(self.output_path)

        union_df = origin_df.union(new_df)
        grouping_df = Window.partitionBy("traceId").orderBy(col("etl_dtm").desc)
        deduplicate_df = union_df.withColumn('row_no', row_number().over(grouping_df)) \
            .filter(col("row_no") == 1).drop('row_no')
        return deduplicate_df


    def run(self):
        df = self.load()
        df.show(n =10,truncate=False)
        processed_df = self.process(df)
        self.write(processed_df)


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