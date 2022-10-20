from pyspark.sql import SparkSession, DataFrame


class SandboxPipeline:

    def __init__(self, spark: SparkSession, raw_zone_path, curated_zone_path):
        self.spark = spark
        self.raw_zone_path = raw_zone_path
        self.curated_zone_path = curated_zone_path

    def execute(self) -> None:
        self.__load(self.__transform(self.__extract()))

    def __extract(self) -> DataFrame:
        print("")

    def __transform(self, extracted_df: DataFrame) -> DataFrame:
        return extracted_df

    def __load(self, transformed_df: DataFrame) -> None:
        transformed_df

