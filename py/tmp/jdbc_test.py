from common.parquet2db import JDBC
from common.SparkSess import SessionGen


spark = SessionGen().create_session(app_name="member_place_recommend", local=False)

jdbc = JDBC(spark)

# S3 Parquet 파일 경로
s3_path = "s3a://tripcok/processed_data/cre_dtm=2024-12-28/"

# Parquet 파일 읽기
df = spark.read.parquet(s3_path)

jdbc.create(df,"tripcok_db", "memberPlaceRecommend")
df = jdbc.read("tripcok_db", "memberPlaceRecommend")
df.show()