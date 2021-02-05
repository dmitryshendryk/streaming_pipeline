
import threading, time
from kafka import KafkaConsumer

class Consumer(threading.Thread):
    def __init__(self, host, port):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.consumer = KafkaConsumer(bootstrap_servers=host + ':' + port,
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=1000)

    def stop(self):
        self.stop_event.set()

    def run(self, topic):
        
        self.consumer.subscribe([topic])

        while not self.stop_event.is_set():
            for message in self.consumer:
                print(message)
                if self.stop_event.is_set():
                    break

        self.consumer.close()