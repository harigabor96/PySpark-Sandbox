from init.Conf import Conf
from init.Router import Router
from pyspark.sql import SparkSession


if __name__ == '__main__':
    conf = Conf("local", "../storage/curated/", "sandbox-pipeline", "../storage/raw/")

    spark = SparkSession\
        .builder\
        .appName("PySpark-Sandbox")\
        .config("spark.sql.warehouse.dir", conf.curated_zone_path)\
        .master(conf.master)\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    Router.execute_pipeline(spark, conf)
