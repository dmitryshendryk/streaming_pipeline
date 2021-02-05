import threading, time

from kafka import KafkaProducer


class Producer(threading.Thread):
    def __init__(self, host, port):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.producer = KafkaProducer(bootstrap_servers=host + ':' + port)

    def stop(self):
        self.stop_event.set()

    def run(self, topic, message):
        
        while not self.stop_event.is_set():
            self.producer.send(topic, message)

        self.producer.close()




