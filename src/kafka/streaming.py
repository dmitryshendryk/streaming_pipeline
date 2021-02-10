import pandas as pd
import os


from src.kafka.consumer import Consumer
from src.kafka.producer import Producer


class StreamingPipeline():
    def __init__(self, configurator, sc) -> None:
        
        self.producer = Producer(configurator, sc)
        self.consumer = Consumer(configurator, sc)
        
    

    def start_streaming(self, topic):
        metadata = pd.read_csv(os.path.join(os.getcwd(), 'notebooks/metadata_cutted.csv'))
        review = pd.read_csv(os.path.join(os.getcwd(), 'notebooks/review_data_cutted.csv'))
        metadata = metadata.head(1)
        review = review.head(1)
        input_data = {
            'metadata': metadata.to_json(orient="split", index=False),
            'review': review.to_json(orient="split", index=False)
        }
        self.producer.run(topic, input_data)
        self.consumer.run(topic)
