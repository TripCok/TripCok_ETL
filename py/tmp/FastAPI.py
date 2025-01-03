import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import to_timestamp, col
from pyspark.sql.types import IntegerType, StringType, TimestampType, DoubleType, LongType

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

df = df.dropna()
# ml_mapping_id가 숫자인 값만 필터링 (정규식을 사용하여 숫자인 값만 필터링)
df_filtered = df.filter(col("ml_mapping_id").rlike("^[0-9]+$"))

# ml_mapping_id를 Integer로 변환할 수 있으면 변환하고, 불가능한 경우에는 null로 처리
df_cleaned = df_filtered.withColumn("ml_mapping_id", col("ml_mapping_id").cast("int"))

#df_cleaned.show()
df_cleaned = df_cleaned.drop("ml_mapping_id")
df_cleaned.show()
# # 나머지 컬럼들을 적절한 데이터 타입으로 변환
# df = df.withColumn("ml_mapping_id", df["ml_mapping_id"].cast(IntegerType())) \
#     .withColumn("create_time", df["create_time"].cast(TimestampType())) \
#     .withColumn("id", df["id"].cast(LongType())) \
#     .withColumn("update_time", df["update_time"].cast(TimestampType())) \
#     .withColumn("address", df["address"].cast(StringType())) \
#     .withColumn("latitude", df["latitude"].cast(DoubleType())) \
#     .withColumn("longitude", df["longitude"].cast(DoubleType())) \
#     .withColumn("name", df["name"].cast(StringType())) \
#     .withColumn("description", df["description"].cast(StringType()))
#
# df_cleaned = df.filter(df["ml_mapping_id"].isNotNull())

#
# df.show()