from pyspark.sql import SparkSession
from common.SparkSess import SessionGen
from common.parquet2db import JDBC
from datetime import datetime, timedelta
from pyspark.sql.functions import col, lit, date_sub,current_date,count

spark = SessionGen().create_session(app_name="member_place_recommend", local=False)
jdbc = JDBC(spark)

# 가장 최근 날짜

execute_date = "2024-12-31"

execute_date_obj = datetime.strptime(execute_date, "%Y-%m-%d")

# 날짜 범위 생성 (execute_date부터 5일 이전까지)
date_range = [(execute_date_obj - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(6)]

# 3. 동적 S3 경로 생성
s3_base_path = "s3a://tripcok/processed_data/"
s3_paths = [f"{s3_base_path}cre_dtm={date}/" for date in date_range]

# 4. S3에서 데이터 읽기 및 결합
dataframes = [spark.read.parquet(path) for path in s3_paths]

# 만약 cre_dtm이 누락되었다면 추가
for i, date in enumerate(date_range):
    dataframes[i] = dataframes[i].withColumn("cre_dtm", lit(date))

combined_data = dataframes[0]

# 데이터프레임 결합
for df in dataframes[1:]:
    combined_data = combined_data.union(df)

# 5. 결과 데이터 확인
combined_data.show()
combined_data.groupBy("cre_dtm").agg(
    count("*").alias("record_count")  # 각 날짜별 데이터 개수
).show()

jdbc.create(combined_data,"tripcok_db", "memberPlaceRecommend")
