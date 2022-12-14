from src.etl.zones import *
from src.init import Conf
from pyspark.sql import SparkSession

class SparkApp:

    @staticmethod
    def run(spark: SparkSession, conf: Conf):
        if conf.pipeline == "sandbox-pipeline":
           bronzesilvergold.tables.sandbox_table \
               .Pipeline(spark, conf).execute()
        elif conf.pipeline == "sandbox-query":
           platinum.queries.sandbox_query \
               .Query(spark, conf).execute().show(20, False)
        else:
            raise Exception("Pipeline is not registered in the router!")
