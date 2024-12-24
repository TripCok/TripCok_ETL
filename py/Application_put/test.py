import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col
from pyspark.sql import functions as F
import os


# 환경변수 읽기
db_url = os.getenv("DB_URL")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")


# Spark 세션 생성
spark = SparkSession.builder \
    .appName("Category Cleansing") \
    .config("spark.driver.extraClassPath", "/home/ubuntu/spark/jars/postgresql-42.7.2.jar") \
    .config("spark.executor.extraClassPath", "/home/ubuntu/spark/jars/postgresql-42.7.2.jar") \
    .getOrCreate()
#paruqet 읽기

folder_path="s3a://tripcok/processed_data/20241222/T/url=%2Fapi%2Fv1%2Fapplication/method=PUT/"

#df = spark.read.parquet(folder_path)

schema = StructType([
    StructField("requestTime", TimestampType(), True),
    StructField("memberId", IntegerType(), True),
    StructField("groupAdminId", IntegerType(), True),
    StructField("applicationId", IntegerType(), True)
])

jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", "tripcok_db.requests") \
    .option("user", db_user) \
    .option("password", db_password) \
    .load()

jdbcDF1 = jdbcDF
jdbcDF.show(truncate=False)
jdbcDF.printSchema()

jdbcDF1.write \
    .format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", "tripcok_db.requests") \
    .option("user", db_user) \
    .option("password", db_password) \
    .mode("append") \
    .save()

jdbcDF2 = spark.read \
    .format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", "tripcok_db.requests") \
    .option("user", db_user) \
    .option("password", db_password) \
    .load()

jdbcDF2.show(truncate=False)

