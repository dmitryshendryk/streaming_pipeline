#!/usr/bin/env python
# coding: utf-8


from pyspark.sql.functions import *
import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql.window import Window
from pyspark.sql.functions import explode, from_unixtime, row_number, year, month

import time



class SparkService():
    def __init__(self, metadata_path, review_path):
        self.metadata_path = metadata_path
        self.review_path = review_path

    def initialize_params(self, partitions = 2100, cores = 5, memory = 11):
        conf = SparkConf()
        conf.set('spark.sql.shuffle.partitions', str(partitions))
        conf.set("spark.executor.cores", str(cores))
        SparkContext.setSystemProperty('spark.executor.memory', str(memory) + 'g')
        SparkContext.setSystemProperty('spark.driver.memory', str(memory) + 'g')
        self.sc = SparkContext(appName='mm_exp', conf=conf)
        self.sqlContext = pyspark.SQLContext(self.sc)

    def process_inquiries(self):
        self.review = self.sqlContext.read.json(self.review_path)
        self.metadata = self.sqlContext.read.json(self.metadata_path)
        self.review_transform_date = self.review.select('asin', 'overall', 'unixReviewTime').withColumn("unixReviewTime", from_unixtime("unixReviewTime"))
        self.review_date_decompose = self.review_transform_date.withColumn("month", month("unixReviewTime")).withColumn("year", year("unixReviewTime"))
        self.metadata_flatten_categories = self.metadata.select('asin', explode('categories')).select('asin', explode('col'))
        self.join_review_metadata = self.review_date_decompose.join(self.metadata_flatten_categories, on=['asin'], how='inner')
        self.groupby_review_metadata = self.join_review_metadata.groupBy("year", "month", "col").count().orderBy('year', 'month', 'count', ascending=False).cache()
        self.patrions = self.groupby_review_metadata.withColumn("rank", row_number().over(self.get_partitions())).cache()
        self.filter_patrions = self.patrions.filter(self.patrions.rank <= 5).cache()
        self.groupby_review_metadata.unpersist()
        self.result_inner = self.join_review_metadata.join(self.filter_patrions, on=['year', 'month', 'col'], how='inner')
        self.patrions.unpersist()
        self.filter_patrions.unpersist()
        self.result_groupby = self.result_inner.groupBy('year', 'month', 'col').avg('overall').orderBy('year', 'month', ascending=True)
        self.result_groupby.show()

    def save(self, path):
        pass

    def stop_spark_context(self):
        self.sc.stop()

    @staticmethod
    def get_partitions():
        windowSpec = Window().partitionBy(['year', 'month']).orderBy('count')
        return windowSpec



# In[38]:


if __name__ == '__main__':
    SparkInstance = SparkService('/amazon/data/metadata.json.gz', '/amazon/data/item_dedup.json.gz')
    SparkInstance.initialize_params()
    SparkInstance.process_inquiries()
    ##here need to save
    SparkInstance.stop_spark_context()

