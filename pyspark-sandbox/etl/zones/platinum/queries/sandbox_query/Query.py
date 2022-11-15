from etl.utils import HiveHelper
from pyspark.sql import SparkSession, DataFrame


class Query:

    def __init__(self, spark: SparkSession, curated_zone_path):
        self.spark = spark
        self.curated_zone_path = curated_zone_path

    def execute(self) -> DataFrame:
        HiveHelper.setup_metastore(self.spark, self.curated_zone_path, "sandboxdb", "sandboxtable")
        return \
            self.spark.sql(f"""
              SELECT *
              FROM sandboxdb.sandboxtable
            """)
