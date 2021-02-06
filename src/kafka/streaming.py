import time

from src.kafka.consumer import Consumer
from src.kafka.producer import Producer


class StreamingPipeline():
    def __init__(self, host, port, topic) -> None:
        self.producer = Producer(host, port)
        self.consumer = Consumer(host, port)
        self.topic = topic
    

    def start_streaming(self):
        self.producer.run(self.topic)
        self.consumer.run(self.topic)
