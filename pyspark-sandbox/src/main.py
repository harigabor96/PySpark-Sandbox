import sys
from init import *
from pyspark.sql import SparkSession

def main():
    # conf = ConfFactory.get_conf(sys.argv)

    conf = ConfFactory.get_conf([
        "-r", "../../storage/raw/",
        "-c", "../../storage/curated/",
        #"-p", "sandbox-pipeline",
        "-p", "sandbox-query"
    ])

    spark = SparkSession \
        .builder \
        .appName("PySpark-Sandbox") \
        .config("spark.jars.packages", "io.delta:delta-core_2.13:2.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", conf.curated_zone_path) \
        .master("local") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    SparkApp.run(spark, conf)

if __name__ == '__main__':
    main()