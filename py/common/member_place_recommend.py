import logging
import sys
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))

class member_place_recommend():

    def __init__(self):
        super().__init__()
        self.run_env = "local"
        self.read_table = None
        self.READ_TABLE_PROD = "bdp_wh.path"
        self.READ_TABLE_LOCAL = "path"
        self.WRITE_TABLE_PROD = "bdp_wh.seller_sales_agg"
        self.WRITE_TABLE_LOCAL = "seller_sales_agg"

    def create_session(self):


    def read(self) -> DataFrame:
        self.read_table = self.READ_TABLE_PROD if self.run_env == "prod" else self.READ_TABLE_LOCAL

        df = self.spark

    def process(self) -> None:
        return None
    def write(self) -> None:
        return None
    def _deduplicate(self) -> None:
        return None


if __name__ == "__main__":
    try:
        member_place_recommend().run()
        logger.info('ETL Job Completed Successfully')
        sys.exit(0)
    except Exception as e:
        logger.error(f"ETL Job Failed: {e}")
        sys.exit(1)