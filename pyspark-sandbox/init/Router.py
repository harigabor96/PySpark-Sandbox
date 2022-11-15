from etl.zones import *
from init import Conf
from pyspark.sql import SparkSession


class Router:

    @staticmethod
    def execute_pipeline(spark: SparkSession, conf: Conf):
        if conf.pipeline == "sandbox-pipeline":
           bronzesilvergold.tables.sandbox_table \
               .Pipeline(spark, conf.raw_zone_path).execute()
        elif conf.pipeline == "sandbox-query":
           platinum.queries.sandbox_query \
               .Query(spark).execute().show(20, False)
        else:
            raise Exception("Pipeline is not registered in the router!")
