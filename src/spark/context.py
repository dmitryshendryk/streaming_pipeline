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

class AppSparkContext():
    def __init__(self, metadata_path, review_path, configurator):
        self.metadata_path = metadata_path
        self.review_path = review_path
        self.mongo = MongoManager(configurator)
        self.session = None

    def initialize_params(self, partitions = 2100, cores = 5, memory = 11):
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

        # conf = pyspark.SparkConf().setAll([('spark.executor.memory', '8g'), ('spark.executor.cores', '3'), ('spark.cores.max', '3'), ('spark.driver.memory','8g')])

        self.session = SparkSession.builder \
                                   .appName("myApp") \
                                   .config(conf=conf) \
                                   .getOrCreate()
        logging.info('Session Created ')
        # SparkContext.setSystemProperty('spark.executor.memory', str(memory) + 'g')
        # SparkContext.setSystemProperty('spark.driver.memory', str(memory) + 'g')
        # self.sc = SparkContext(appName='mm_exp', conf=conf).getOrCreate()
        # self.sqlContext = pyspark.SQLContext(self.sc)

    def process_inquiries(self):
        logging.info("Start pipeline")
        logging.info("Reading review data")
        review = self.session.read.json(self.review_path)
        logging.info("Reading metadata")
        metadata = self.session.read.json(self.metadata_path)
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
        self.save(result_groupby, 'mydb', 'myset')
    
    def read_file(self, path):
        print(self.session)
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
