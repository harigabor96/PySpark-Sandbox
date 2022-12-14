from src.etl.utils import HiveHelper
from src.init import Conf
from pyspark.sql import SparkSession, DataFrame

class Query:

    def __init__(self, spark: SparkSession, conf: Conf):
        self.spark = spark
        self.conf = conf

    def execute(self) -> DataFrame:
        HiveHelper.setup_metastore(self.spark, "sandboxdb", "sandboxtable")
        return \
            self.spark.sql(f"""
              SELECT *
              FROM sandboxdb.sandboxtable
            """)
