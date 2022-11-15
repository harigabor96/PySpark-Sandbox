from pyspark.sql import SparkSession


class HiveHelper:

    @staticmethod
    def setup_metastore(spark: SparkSession, database_name: str, table_name: str):
        spark.sql(f"""
            CREATE DATABASE IF NOT EXISTS {database_name};
        """)
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
            USING DELTA
            LOCATION '{spark.conf.get("spark.sql.warehouse.dir")}/{database_name}.db/{table_name}/data';
        """)
