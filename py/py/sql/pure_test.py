import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col
from pyspark.sql import functions as F
import pymysql

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
    StructField("requestTime", StringType(), True),
    StructField("memberId", IntegerType(), True),
    StructField("groupAdminId", IntegerType(), True),
    StructField("applicationId", IntegerType(), True)
])

jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://13.209.89.20:5432/tripcok_db") \
    .option("dbtable", "tripcok_db.requests") \
    .option("user", "postgres") \
    .option("password", "tripcok1234") \
    .load()

jdbcDF.show(truncate=False)

jdbcDF.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://13.209.89.20:5432/tripcok_db") \
    .option("dbtable", "tripcok_db.requests") \
    .option("user", "postgres") \
    .option("password", "tripcok1234") \
    .mode("overwrite") \
    .save()

jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://13.209.89.20:5432/tripcok_db") \
    .option("dbtable", "tripcok_db.requests") \
    .option("user", "postgres") \
    .option("password", "tripcok1234") \
    .load()

jdbcDF.show(truncate=False)

