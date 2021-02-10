
import logging
import threading
import time
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from src.db.mongodb.db_manager import MongoManager
from config.configurator import Configurator
from src.spark.context import AppSparkContext


class Consumer(threading.Thread):
    def __init__(self, configurator: Configurator, context: AppSparkContext) -> None:
        host = configurator['clusters']['kafka']['host']
        port = configurator['clusters']['kafka']['port']

        self.ssc = StreamingContext(
            context.session.sparkContext, batchDuration=20)
        self.broker = host + ':' + port
        self.mongo = MongoManager(configurator)
        self.context = context

    def stop(self):
        self.stop_event.set()

    def run(self, topic: str) -> None:

        def handler(message) -> None:
            records = message.collect()
            logging.info("Handling Data")
            if len(records) > 0:
                data = json.loads(records[-1][-1])
                metadata = data['metadata']
                review = data['review']

                review_df = self.context.session.read.json(
                    self.context.session.sparkContext.parallelize([review]))
                metadata_df = self.context.session.read.json(
                    self.context.session.sparkContext.parallelize([metadata]))
                self.context.process_inquiries(review_df, metadata_df)
            else:
                logging.info("Topic is empty")

        logging.info('Run Consumer')
        kvs = KafkaUtils.createDirectStream(self.ssc, [topic], kafkaParams={
                                            "metadata.broker.list": self.broker})

        kvs.foreachRDD(handler)

        self.ssc.start()
        self.ssc.awaitTermination()
