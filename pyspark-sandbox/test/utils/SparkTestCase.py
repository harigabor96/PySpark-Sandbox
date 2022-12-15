import unittest
from pyspark.sql import SparkSession

class SparkTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = \
            SparkSession \
                .builder \
                .appName("Test") \
                .master("local") \
                .config("spark.sql.warehouse.dir", "../../storage/curated/") \
                .config("spark.jars.packages", "io.delta:delta-core_2.13:2.1.0") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .getOrCreate()

        cls.spark \
            .sparkContext.setLogLevel("ERROR")
