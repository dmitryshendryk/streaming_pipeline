import logging
import threading
import time
import json
import random
import pandas as pd
import os

from pykafka import KafkaClient

from config.configurator import Configurator
from src.spark.context import AppSparkContext


class Producer(threading.Thread):
    def __init__(self, configurator: Configurator, sc: AppSparkContext):
        host = configurator['clusters']['kafka']['host']
        port = configurator['clusters']['kafka']['port']
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.client = KafkaClient(hosts=host + ':' + port)

    def stop(self) -> None:
        self.stop_event.set()

    def run(self, topic: str, data) -> None:
        logging.info('Run Producer')
        topic = self.client.topics[bytes(topic, encoding='utf-8')]
        producer = topic.get_producer()
        num_user = 2

        def work():
            while True:
                msg = json.dumps(data)
                logging.info('msg from producer: %s', str(len(msg)))
                producer.produce(bytes(msg, encoding='utf-8'))
                time.sleep(20)

        thread_list = [threading.Thread(target=work) for i in range(num_user)]
        for thread in thread_list:
            thread.setDaemon(True)
            thread.start()
