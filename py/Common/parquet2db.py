import os
import logging
from pyspark.sql import SparkSession
from common.SparkSess import SessionGen
from datetime import datetime, timedelta
from pyspark.sql.functions import col, lit, date_sub,current_date,count
# 로그 설정
logging.basicConfig(level=logging.INFO)


class SparkSessionManager:
    """Spark 세션을 관리하는 클래스."""
    def __init__(self, app_name: str, driver_classpath: str, executor_classpath: str):
        self.app_name = app_name
        self.driver_classpath = driver_classpath
        self.executor_classpath = executor_classpath
        self.spark = SessionGen().create_session(app_name="member_place_recommend", local=False)
    def get_spark_session(self):
        """Spark 세션 생성 및 반환."""
        return SparkSession.builder \
            .appName(self.app_name) \
            .config("spark.driver.extraClassPath", self.driver_classpath) \
            .config("spark.executor.extraClassPath", self.executor_classpath) \
            .getOrCreate()


class JDBC:
    """데이터베이스와 상호작용을 관리하는 클래스."""
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.db_url = os.getenv("AGG_DB_URL")  # 기본값 설정 가능
        self.db_user = os.getenv("AGG_DB_USER")
        self.db_password = os.getenv("AGG_DB_PASSWORD")
        self.db_schema = os.getenv("AGG_DB_SCHEMA")

    def read(self, db_schema : str, table_name: str):
        """데이터베이스에서 데이터를 읽어오는 메서드."""
        logging.info(f"Reading data from table: {table_name}")
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.db_url) \
            .option("dbtable", f"{db_schema}.{table_name}") \
            .option("user", self.db_user) \
            .option("password", self.db_password) \
            .load()

    def write(self, df, table_name: str, column_types: str = None, mode: str = "overwrite"):
        """데이터를 데이터베이스에 저장하는 메서드."""
        logging.info(f"Writing data to table: {table_name}")
        writer = df.write \
            .format("jdbc") \
            .option("url", self.db_url) \
            .option("dbtable", f"{self.db_schema}.{table_name}") \
            .option("user", self.db_user) \
            .option("password", self.db_password) \
            .mode(mode)

        if column_types:
            writer.option("createTableColumnTypes", column_types)

        writer.save()

    def create(self, df, db_schema, table_name: str, mode: str = "overwrite"):
        """데이터를 데이터베이스에 저장하는 메서드."""
        logging.info(f"Writing data to table: {table_name}")

        # DataFrame을 지정한 PostgreSQL 테이블에 쓰기
        df.write \
            .format("jdbc") \
            .option("url", self.db_url) \
            .option("dbtable", f"{db_schema}.{table_name}") \
            .option("user", self.db_user) \
            .option("password", self.db_password) \
            .mode(mode) \
            .save()

    def storeDataForPeriod(self, s3_base_path : str, partitioned_column : str, start_date : str, period : int, table_name: str ):
        """
        주어진 기간 동안 S3 경로에서 데이터를 읽어와 병합한 후 데이터베이스에 저장합니다.

        Args:
            s3_base_path (str): S3 데이터가 저장된 기본 경로.
            paritioned_column (str): 파티션을 나타내는 컬럼명 (예: 날짜 컬럼).
            start_date (str): 시작 날짜 (YYYY-MM-DD 형식).
            period (int): 처리할 기간 (일 단위).
            table_name (str): 저장할 데이터베이스 테이블 이름.
        """

        execute_date_obj = datetime.strptime(start_date, "%Y-%m-%d")

        date_range = [(execute_date_obj - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(period)]

        s3_paths = [f"{s3_base_path}/{partitioned_column}={date}/" for date in date_range]

        dataframes = [self.spark.read.parquet(path) for path in s3_paths]

        # 만약 cre_dtm이 누락되었다면 추가
        for i, date in enumerate(date_range):
            dataframes[i] = dataframes[i].withColumn("cre_dtm", lit(date))

        combined_data = dataframes[0]

        # 데이터프레임 결합
        for df in dataframes[1:]:
            combined_data = combined_data.union(df)

        self.create(combined_data, "tripcok_db", f"{table_name}")
        self.read("tripcok_db", f"{table_name}").show()
        return combined_data

class S3:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session

    def read(self, s3_base_path: str, partitioned_column: str, date: str, query: str, table_name: str):

        s3_path = f"{s3_base_path}/{partitioned_column}={date}/"

        df = self.spark.read.parquet(s3_path)

        # 임시 뷰 생성
        df.createOrReplaceTempView(f"{table_name}")

        # 쿼리 실행
        filtered_df = self.spark.sql(query)

        return filtered_df


    def readForPeriod(self, s3_base_path : str, partitioned_column : str, start_date : str, period : int ):

        execute_date_obj = datetime.strptime(start_date, "%Y-%m-%d")

        date_range = [(execute_date_obj - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(period)]

        s3_paths = [f"{s3_base_path}/{partitioned_column}={date}/" for date in date_range]

        dataframes = [self.spark.read.parquet(path) for path in s3_paths]

        # 만약 cre_dtm이 누락되었다면 추가
        for i, date in enumerate(date_range):
            dataframes[i] = dataframes[i].withColumn("cre_dtm", lit(date))

        combined_data = dataframes[0]

        # 데이터프레임 결합
        for df in dataframes[1:]:
            combined_data = combined_data.union(df)

        return combined_data


# 실행 예제
if __name__ == "__main__":

    app = JDBC()
    app.run()
