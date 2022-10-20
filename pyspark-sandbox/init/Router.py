from etl.SandboxPipeline import SandboxPipeline
from init.Conf import Conf
from pyspark.sql import SparkSession


class Router:

    @staticmethod
    def execute_pipeline(spark: SparkSession, conf: Conf):
        if conf.pipeline == "sandbox-pipeline":
            SandboxPipeline(spark, conf.raw_zone_path, conf.curated_zone_path).execute()
        else:
            raise Exception("Pipeline is not registered in the router!")
