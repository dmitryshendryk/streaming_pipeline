import os
import sys
import click
import logging
from typing import Dict, Optional, Tuple

from config.configurator import Configurator
from src.kafka.streaming import StreamingPipeline
from src.spark.context import AppSparkContext

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

# TODO for stream need specify topics which we will read, so we need create topics and stream
# TODO check spark paralelling
# TODO scale kafka 
@service.command(help='stream_pipeline')
@click.pass_context
def stream_pipeline(context: click.core.Context):
    logging.info("Kafka -> Spark -> MongoDB")
    project_root = context.obj['PROJECT_ROOT']
    configurator = get_configurator(project_root)._configuration_data
    context = AppSparkContext()
    st = StreamingPipeline(configurator, context)
    st.start_streaming('my_topic')
    
# TODO load heavy files and store to MongoDB
# TODO get top 5 categories which contain most sold products month by month
@service.command(help='io_pipeline')
@click.pass_context
def io_pipeline(context: click.core.Context):
    logging.info("IO -> Spark -> MongoDB")
    project_root = context.obj['PROJECT_ROOT']
    context = AppSparkContext()
    configurator = get_configurator(project_root)._configuration_data



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