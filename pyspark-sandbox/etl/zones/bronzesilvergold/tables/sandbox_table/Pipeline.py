from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType


class Pipeline:

    def __init__(self, spark: SparkSession, raw_zone_path, curated_zone_path):
        self.spark = spark
        self.curated_zone_path = curated_zone_path

        self.input_path = raw_zone_path + "/{*}"
        self.input_schema = StructType([
            StructField("sandbox_field", StringType(), True)
        ])

        self.output_database_name = "sandboxdb"
        self.output_table_name = "sandboxtable"
        self.output_data_relative_path = f"{self.output_database_name}.db/{self.output_table_name}/data"
        self.output_checkpoint_relative_path = f"{self.output_database_name}.db/{self.output_table_name}/checkpoint"

    def execute(self) -> None:
        self.__load(self.__transform(self.__extract()))

    def __extract(self) -> DataFrame:
        return self.spark \
            .readStream \
            .option("sep", ";") \
            .option("header", "true") \
            .schema(self.input_schema) \
            .csv(self.input_path)

    def __transform(self, extracted_df: DataFrame) -> DataFrame:
        return extracted_df

    def __load(self, transformed_df: DataFrame) -> None:
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.output_database_name}")

        transformed_df \
            .writeStream \
            .trigger(availableNow=True) \
            .outputMode("append") \
            .format("delta") \
            .option("path", self.output_data_relative_path) \
            .option("checkpointLocation", f"{self.curated_zone_path}/{self.output_checkpoint_relative_path}") \
            .toTable(f"{self.output_database_name}.{self.output_table_name}") \
            .awaitTermination()
