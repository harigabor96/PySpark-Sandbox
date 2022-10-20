from init.Conf import Conf
from pyspark.sql import SparkSession


class Router:

    @staticmethod
    def execute_pipeline(spark: SparkSession, conf: Conf):
        pipeline = conf.pipeline

        if pipeline == "sandbox-pipeline":
            print("sandbox pipeline!")
        else:
            raise Exception("Pipeline is not registered in the router!")
