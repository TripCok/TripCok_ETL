from pyspark.sql import SparkSession
from common.SparkSess import SessionGen
from common.parquet2db import JDBC
from datetime import datetime, timedelta
from pyspark.sql.functions import col, lit, date_sub,current_date,count

spark = SessionGen().create_session(app_name="member_place_recommend", local=False)
jdbc = JDBC(spark)

date = datetime.today().strftime('%Y-%m-%d')  # 오늘 날짜를 'YYYY-MM-DD' 형식으로 저장

print(f" 오늘 날짜는 {date}입니다")

data = jdbc.storeDataForPeriod(s3_base_path="s3a://tripcok/processed_data",
                        start_date=date,
                        period=5,
                        partitioned_column="cre_dtm",
                        table_name="memberPlaceRecommend")

data.groupBy("cre_dtm").agg(
     count("*").alias("record_count")  # 각 날짜별 데이터 개수
).show()