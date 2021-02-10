import logging
import pyspark
import os
from pyspark import SparkConf

from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession

import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql.window import Window
from pyspark.sql.functions import explode, from_unixtime, row_number, year, month
from src.db.mongodb.db_manager import MongoManager
from config.configurator import Configurator

class AppSparkContext():
    def __init__(self, configurator: Configurator):
        self.mongo = MongoManager(configurator)
        partitions = configurator['clusters']['spark']['partitions']
        cores = configurator['clusters']['spark']['cores']
        memory = configurator['clusters']['spark']['memory']
        logging.info('Initialize Params')
        conf = SparkConf()
        working_directory = os.path.join(os.getcwd(), 'libs/jars/*') 

        conf.set('spark.sql.shuffle.partitions', str(partitions))
        conf.set("spark.executor.cores", str(cores))
        conf.set("spark.executor.memory", str(memory) + 'g')
        conf.set("spark.driver.memory", str(memory) + 'g')
        conf.set("spark.mongodb.output.uri", "mongodb://127.0.0.1/mydb.myCollection")
        conf.set("spark.mongodb.input.uri", "mongodb://127.0.0.1/mydb.myCollection")
        conf.set("spark.driver.extraClassPath", working_directory)

        try:
            self.session = SparkSession.builder \
                                    .appName("myApp") \
                                    .config(conf=conf) \
                                    .getOrCreate()
        except Exception as e:
            logging.info('Session creation failed %s', e)

    def process_inquiries(self, review, metadata):
        logging.info("Start pipeline")

        logging.info("Processing")
        review_transform_date = review.select('asin', 'overall', 'unixReviewTime').withColumn("unixReviewTime", from_unixtime("unixReviewTime"))
        review_date_decompose = review_transform_date.withColumn("month", month("unixReviewTime")).withColumn("year", year("unixReviewTime"))
        metadata_flatten_categories = metadata.select('asin', explode('categories')).select('asin', explode('col'))
        join_review_metadata = review_date_decompose.join(metadata_flatten_categories, on=['asin'], how='inner')
        groupby_review_metadata = join_review_metadata.groupBy("year", "month", "col").count().orderBy('year', 'month', 'count', ascending=False).cache()
        patrions = groupby_review_metadata.withColumn("rank", row_number().over(self.get_partitions())).cache()
        filter_patrions = patrions.filter(self.patrions.rank <= 5).cache()
        groupby_review_metadata.unpersist()
        result_inner = join_review_metadata.join(filter_patrions, on=['year', 'month', 'col'], how='inner')
        patrions.unpersist()
        filter_patrions.unpersist()
        result_groupby = result_inner.groupBy('year', 'month', 'col').avg('overall').orderBy('year', 'month', ascending=True)
        result_groupby.show()
        logging.info("Finished")
        self.save(result_groupby, 'mydb', 'myset')
    
    def read_file(self, path):
        logging.info("Reading data")
        df = self.session.read.json(path)
        return df

    
    def save(self, df, db, collection):
        self.mongo.insert_spark_df(df, db, collection)

    def stop_spark_context(self):
        self.session.stop()

    @staticmethod
    def get_partitions():
        windowSpec = Window().partitionBy(['year', 'month']).orderBy('count')
        return windowSpec
