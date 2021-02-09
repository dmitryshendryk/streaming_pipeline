import logging
import os

import pymongo
from pymongo import MongoClient
from pyspark.sql import SparkSession

class MongoManager():
    def __init__(self, configurator) -> None:
        self.working_directory = os.path.join(os.getcwd(), 'libs/jars/*') 
        self.configurator = configurator
        self.host = configurator['databases']['mongodb']['dev']['host']
        self.port = configurator['databases']['mongodb']['dev']['port']
        

    def insert_data(self, data_dict:dict):
        conn = MongoClient(self.host, self.port)
        db = conn.mydb
        try:
            db.myset.insert(data_dict)
        except Exception as e:
            logging.error('Insert to mongo %s', e)
        finally:
            conn.close()

    def insert_spark_df(self, df, database, collection):

        try:
            
            df.write.format("com.mongodb.spark.sql.DefaultSource") \
                        .option("database", database) \
                        .option("collection", collection) \
                        .mode("append").save()
            
        except Exception as e:
            logging.error('Insert spark error %s', e)

    def query_spark_df(self, session, database, collection):

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

    def insert_row(self, x):
        if x is None or len(x)<1:
            return
        data_list=x.split(',')
        self.insert_data({
                    'timestamp': data_list[0],
                    'uid': data_list[1],
                    'heart_rate': data_list[2],
                    'steps': data_list[3]
        })