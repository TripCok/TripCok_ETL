import os
import logging
from pyspark.sql import SparkSession
from common.SparkSess import SessionGen
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




# 실행 예제
if __name__ == "__main__":

    app = JDBC()
    app.run()
