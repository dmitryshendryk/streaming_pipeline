import os
import sys
import click
import logging
from typing import Dict, Optional, Tuple

from config.configurator import Configurator
from src.kafka.streaming import StreamingPipeline
from src.spark.context import AppSparkContext
from src.cron.manager import CronTab

os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars ' + os.path.join(os.getcwd(), 'libs/spark-streaming-kafka-0-8-assembly_2.11-2.4.6.jar') + ' pyspark-shell' 

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
PROJECT_ROOT = os.path.join(PROJECT_ROOT)

sys.path.append(PROJECT_ROOT)

def get_configurator(project_root_path: str):
    config_path = os.path.join(project_root_path, "config/configuration.yaml")
    return Configurator(config_path)

def configure_logging(logging_format: str = 'Date-Time : %(asctime)s : Line No. : %(lineno)d - %(message)s') -> None:
    logging.basicConfig(
        format=logging_format,
        level=logging.INFO
    )


@click.group()
def service():
    """Group service"""

## MAJOR TASKS
# TODO Proper exception handling/logging for bulletproof runs (3 times a day) - done
# TODO Expressive, re-usable, clean code - done
# TODO Remarks / Comments of those parts (optimization) where you could speed up execution and/or save network traffic
# TODO handle duplicates. - Structure Streaming  
# TODO download the source data from the pipeline itself and have the ability to do the same at regular intervals. - done
# TODO use some scheduling framework or workflow platform. - done
# TODO use containers. 
# TODO handle partial downloads and failures at getting the source data. - done (handled by Spark)
# TODO be scalable in case new data were to flow in on a high-volume basis(10x bigger) and has to be imported at a higher frequency.
# TODO be able to handle the JSON files as a stream/ abstract the file reading into a streaming model.
# TODO describe the data store and tools used to query it - including the presentation layer. - done

# TODO for stream need specify topics which we will read, so we need create topics and stream
# TODO check spark paralelling  - done
# TODO scale kafka 
@service.command(help='stream_pipeline')
@click.pass_context
def stream_pipeline(context: click.core.Context):
    logging.info("Kafka -> Spark -> MongoDB")
    project_root = context.obj['PROJECT_ROOT']
    configurator = get_configurator(project_root)._configuration_data
    context = AppSparkContext(configurator)
    st = StreamingPipeline(configurator, context)
    st.start_streaming('my_topic')
    context.stop_spark_context()


# TODO load heavy files and store to MongoDB - done
# TODO get top 5 categories which contain most sold products month by month - done
# TODO  create cron job  - done
# TODO put mondo  in kubernetis
# TODO build docker image and run it 
# TODO handle duplicates
@service.command(help='io_pipeline')
@click.pass_context
@click.option('--cron', default='', help='Enable cron job')
def io_pipeline(context: click.core.Context, cron: bool):
    logging.info("IO -> Spark -> MongoDB")
    project_root = context.obj['PROJECT_ROOT']
    configurator = get_configurator(project_root)._configuration_data
    context = AppSparkContext(configurator)
    

    # TODO pass params
    if cron:
        CronTab(context.process_inquiries, configurator).start()
    
    ### TEST PIPELINE
    # df = context.read_file('/amazon/data/metadata.json.gz')
    # df = df.limit(10)
    # context.save(df, 'mydb','spark')
    
    #### READIN DATA
    metadata = context.read_file('/amazon/data/metadata.json.gz')
    review = context.read_file('/amazon/data/item_dedup.json.gz')
    #### MAIN PIPELINE 
    context.process_inquiries(review, metadata)
    context.stop_spark_context()



def main(env_variables: Optional[Dict[str, str]] = None) -> None:
    if env_variables is None:
        env_variables = {}

    _environ = os.environ.copy()
    try:
        os.environ.update(env_variables)
        service(obj=env_variables)
    finally:
        os.environ.clear()
        os.environ.update(_environ)


if __name__ == '__main__':
    configure_logging()

    main(env_variables={
        'PROJECT_ROOT': PROJECT_ROOT,
        'PYTHONPATH': os.pathsep.join(sys.path),
    })