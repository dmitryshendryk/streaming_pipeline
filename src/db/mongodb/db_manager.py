import logging
import os

import pymongo
from pymongo import MongoClient
from pyspark.sql import SparkSession
import pyspark


class MongoManager():
    def __init__(self, configurator) -> None:
        self.working_directory = os.path.join(os.getcwd(), 'libs/jars/*')
        self.configurator = configurator
        self.host = configurator['databases']['mongodb']['dev']['host']
        self.port = configurator['databases']['mongodb']['dev']['port']

    def insert_spark_df(self, df: pyspark.sql.DataFrame, database: str, collection: str) -> None:

        try:

            df.write.format("com.mongodb.spark.sql.DefaultSource") \
                .option("database", database) \
                .option("collection", collection) \
                .mode("overwrite").save()

        except Exception as e:
            logging.error('Insert spark error %s', e)

    def query_spark_df(self, session: SparkSession, database: str, collection: str):

        df = None
        try:
            df = session.read.format("com.mongodb.spark.sql.DefaultSource") \
                .option("database", database) \
                .option("collection", collection) \
                .load()
            df.select('*').show()
        except Exception as e:
            logging.error('Query spark error %s', e)

        return df
