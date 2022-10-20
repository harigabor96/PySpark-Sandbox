from pyspark.sql import SparkSession

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    spark = SparkSession\
        .builder\
        .appName("PySpark-Sandbox") \
        .config("spark.sql.warehouse.dir", "..\storage") \
        .master("local") \
        .getOrCreate()
