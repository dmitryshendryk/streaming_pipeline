import time
from kafka.admin import NewTopic
from kafka import KafkaAdminClient


from src.kafka.consumer import Consumer
from src.kafka.producer import Producer

def test_kafka():
    try:
        admin = KafkaAdminClient(bootstrap_servers='localhost:9094')

        topic = NewTopic(name='my_topic',
                         num_partitions=1,
                         replication_factor=1)
        admin.create_topics([topic])
    except Exception as e:
        print(e)
        

    tasks = [
        Producer(),
        Consumer()
    ]

    for t in tasks:
        t.start()

    time.sleep(10)

    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()
