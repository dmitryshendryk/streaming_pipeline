import logging
import threading, time
import json
import random

from pykafka import KafkaClient



class Producer(threading.Thread):
    def __init__(self, host, port):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.client = KafkaClient(hosts=host + ':' + port)
        

    def stop(self):
        self.stop_event.set()

    def run(self, topic):
        logging.info('Run Producer')
        topic = self.client.topics[bytes(topic, encoding='utf-8')]
        producer = topic.get_producer()
        users = ['t1', 't2']
        num_user = 2
        def work(user_number):
            while True:
                msg = json.dumps({
                    'timestamp': str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())),
                    'uid': users[user_number],
                    'heart_rate': random.randint(50, 70),
                    'steps': random.randint(100, 1000)
                })
                logging.info('msg from producer: %s', msg)
                producer.produce(bytes(msg, encoding='utf-8'))
                time.sleep(20)


        thread_list = [threading.Thread(target=work, args=(i,)) for i in range(num_user)]
        for thread in thread_list:
            thread.setDaemon(True)
            thread.start()


