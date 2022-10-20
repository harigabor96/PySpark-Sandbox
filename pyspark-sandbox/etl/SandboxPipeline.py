from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType


class SandboxPipeline:

    def __init__(self, spark: SparkSession, raw_zone_path, curated_zone_path):
        self.spark = spark
        self.raw_zone_path = raw_zone_path
        self.curated_zone_path = curated_zone_path

        self.inputPath = self.raw_zone_path + "/{*}"

        self.inputSchema = StructType([
            StructField("sandbox_field", StringType(), True)
        ])

        self.outputDatabaseName = "sandboxdb"
        self.outputTableName = "sandboxtable"
        self.outputDataRelativePath = f"{self.outputDatabaseName}.db/{self.outputTableName}/data"
        self.outputCheckpointRelativePath = f"{self.outputDatabaseName}.db/{self.outputTableName}/checkpoint"

    def execute(self) -> None:
        self.__load(self.__transform(self.__extract()))

    def __extract(self) -> DataFrame:
        return self.spark\
            .readStream\
            .option("sep", ";")\
            .option("header", "true")\
            .schema(self.inputSchema)\
            .csv(self.inputPath)

    def __transform(self, extracted_df: DataFrame) -> DataFrame:
        return extracted_df

    def __load(self, transformed_df: DataFrame) -> None:
        transformed_df\
            .writeStream\
            .trigger(availableNow=True)\
            .outputMode("append")\
            .format("console")\
            .option("truncate", False)\
            .start()\
            .awaitTermination()
