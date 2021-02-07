import time


from src.kafka.consumer import Consumer
from src.kafka.producer import Producer


class StreamingPipeline():
    def __init__(self, configurator, sc) -> None:
        
        self.producer = Producer(configurator, sc)
        self.consumer = Consumer(configurator, sc)
        
    

    def start_streaming(self, topic):
        self.producer.run(topic)
        self.consumer.run(topic)
