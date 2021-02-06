
import logging
import threading, time
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


class Consumer(threading.Thread):
    def __init__(self, host, port):
        sc=SparkContext(appName='test')

        self.ssc=StreamingContext(sc,batchDuration=20)
        self.broker = host + ':' + port
        

    def stop(self):
        self.stop_event.set()

    def run(self, topic):
        logging.info('Run Consumer')
        kvs=KafkaUtils.createDirectStream(self.ssc,[topic],kafkaParams={"metadata.broker.list":self.broker})
        

        kvs.pprint()
        lines=kvs.map(lambda x:'{},{},{},{}'.format(json.loads(x[1])['timestamp'],json.loads(x[1])['uid'],
                                                json.loads(x[1])['heart_rate'],json.loads(x[1])['steps']))
        # lines.foreachRDD(lambda rdd:rdd.foreach(logging.info(rdd)))

        self.ssc.start()
        self.ssc.awaitTermination()