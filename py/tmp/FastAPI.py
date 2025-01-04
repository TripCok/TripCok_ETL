import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import to_timestamp, col
from pyspark.sql.types import IntegerType, StringType, TimestampType, DoubleType, LongType
from common.parquet2db import S3
# 입력값 {memberId: 1}
# 반환값 {placeId : 1, imagePath : str, contentId : str}
# 형한테 API
# session 만들 때에 mariadb import
#
# Read가 되는 지 확인하기

DRIVER_CLASSPATH = os.getenv("MARIA_DRIVER_CLASSPATH")
EXECUTOR_CLASSPATH = os.getenv("MARIA_EXECUTOR_CLASSPATH")
import pyspark

from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, TimestampType,
    DoubleType, LongType
)


# session 만들기
spark = SparkSession.builder.appName("mariadb") \
                       .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
                       .config("spark.executor.memory", "2g") \
                        .config("spark.executor.cores", "2") \
                       .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
                        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
                        .config("spark.driver.extraClassPath", DRIVER_CLASSPATH) \
                        .config("spark.executor.extraClassPath", EXECUTOR_CLASSPATH).getOrCreate()

jdbc_url = "jdbc:mysql://54.180.151.205:3366/tripcok_db"
properties = {
    "user": "tripcok",
    "password": "tripcok1234"
}


df = spark.read.jdbc(
    url=jdbc_url,
    table="place",
    properties=properties
)

df_image = spark.read.jdbc(
    url=jdbc_url,
    table="place_image",
    properties=properties
)


#df = df_image의 id와 df의 id를 기준으로 조인하기

# recommend_df = S3에서 memberId 기준으로 추출하기 input (S3_path, sql 쿼리로)
s3 = S3(spark)
memberId=5

table_name = "place"
query = f" SELECT * FROM {table_name} WHERE memberId = {memberId} "
s3_base_path ="s3a://tripcok/processed_data"
partitioned_column ="cre_dtm"
date = "2025-01-01"
df = s3.read(s3_base_path, partitioned_column, date, query, table_name)
# df 와 recommend id 기준으로 join하기

# 출력하기

df.show()

#결과적으로 보내줘야 하는 거
# 입력값 {memberId: 1}
# 반환값 {placeId : 1, imagePath : str, contentId : str}