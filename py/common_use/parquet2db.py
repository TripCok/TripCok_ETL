import logging
from pyspark.sql import SparkSession
from SparkSess import SessionGen
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


class PDBC:
    """데이터베이스와 상호작용을 관리하는 클래스."""
    def __init__(self, spark_session: SparkSession, db_config: dict):
        self.spark = spark_session
        self.db_url = db_config["url"]
        self.db_user = db_config["user"]
        self.db_password = db_config["password"]
        self.db_table = db_config.get("table", "")

    def read(self, table_name: str):
        """데이터베이스에서 데이터를 읽어오는 메서드."""
        logging.info(f"Reading data from table: {table_name}")
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.db_url) \
            .option("dbtable", table_name) \
            .option("user", self.db_user) \
            .option("password", self.db_password) \
            .load()

    def write(self, df, table_name: str, column_types: str = None, mode: str = "append"):
        """데이터를 데이터베이스에 저장하는 메서드."""
        logging.info(f"Writing data to table: {table_name}")
        writer = df.write \
            .format("jdbc") \
            .option("url", self.db_url) \
            .option("dbtable", table_name) \
            .option("user", self.db_user) \
            .option("password", self.db_password) \
            .mode(mode)

        if column_types:
            writer.option("createTableColumnTypes", column_types)

        writer.save()




# 실행 예제
if __name__ == "__main__":
    # KST 시간대 정의
    KST = timezone(timedelta(hours=9))

    # 현재 KST 날짜 가져오기
    current_date = datetime.now(KST).strftime('%Y%m%d')
    
    # 명령줄 인자 처리 
    parser = argparse.ArgumentParser(description="Logs Cleansing Pipeline")
    parser.add_argument("--bucket", required=False, default="tripcok", help="S3 bucket name")
    parser.add_argument("--folder", required=False, default=f"processed_data/", help="S3 folder path (prefix)")
    parser.add_argument("--date", required=True, help="Execution date (YYYYMMDD)")
    
    # parser 저장
    args = parser.parse_args()

    app = InsertDb(bucket_name=args.bucket, folder_path=args.folder, execute_date=args.date)
    app.run()
