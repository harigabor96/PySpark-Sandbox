from pyspark.sql import SparkSession


class HiveHelper:

    @staticmethod
    def setup_metastore(spark: SparkSession, curated_zone_path: str, database_name: str, table_name: str):
        spark.sql(f"""
            CREATE DATABASE IF NOT EXISTS {database_name};
        """)
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS $databaseName.$tableName
            USING DELTA
            LOCATION '../{curated_zone_path}/{database_name}.db/{table_name}/data';
        """)
