from init import *
from pyspark.sql import SparkSession


if __name__ == '__main__':
    conf = Conf("local", "../storage/curated/", "sandbox-pipeline", "../storage/raw/")

    spark = SparkSession\
        .builder\
        .appName("PySpark-Sandbox")\
        .config("spark.jars.packages", "io.delta:delta-core_2.13:2.1.0")\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .config("spark.sql.warehouse.dir", conf.curated_zone_path)\
        .master(conf.master)\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    Router.execute_pipeline(spark, conf)
