import glob
import os

from pyspark.sql import SparkSession


class SessionGen():

    def __init__(self):
        spark_home = os.getenv("SPARK_HOME")
        if not spark_home:
            raise EnvironmentError("SPARK_HOME 설정 필요")

        self.LOCAL_JAR_PATH = f"{spark_home}/jars/*.jar"  # SPARK_HOME 경로에서 JAR 파일
        self.PROD_JAR_PATH = "/home/ubuntu/spark/jars/*.jar"  # 프로덕션 JAR 파일 경로

    def create_session(self, app_name=None, local: bool = False):
        os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
        # 기존에 있는 Spark 반환

        spark = SparkSession.builder.appName(app_name) \
                        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
                        .config("spark.executor.memory", "2g") \
                        .config("spark.executor.cores", "2") \
                        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
                        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")

        if local:
            jar_files = glob.glob(self.LOCAL_JAR_PATH)
            builder = spark.master("local").config("spark.jars", ",".join(jar_files))

        else:
            jar_files = glob.glob(self.PROD_JAR_PATH)
            builder = spark.config("spark.jars", ",".join(jar_files))


        return builder.getOrCreate()


