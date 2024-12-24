# test.py
from pyspark.sql import SparkSession

# SparkSession 생성
spark = SparkSession.builder \
    .appName("SimpleSparkApp") \
    .getOrCreate()

# 간단한 RDD 작업 예시
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
rdd_sum = rdd.reduce(lambda a, b: a + b)

print(f"Sum of RDD: {rdd_sum}")

# 작업 완료 후 SparkSession 종료
spark.stop()
