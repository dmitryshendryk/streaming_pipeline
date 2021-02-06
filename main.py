import os
import sys
import click
import logging
from typing import Dict, Optional

from config.configurator import Configurator
from src.kafka.streaming import StreamingPipeline

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


@service.command(help='pipeline')
@click.pass_context
def pipeline(context: click.core.Context):
    project_root = context.obj['PROJECT_ROOT']
    configurator = get_configurator(project_root)._configuration_data
    kafka_host = configurator['clusters']['kafka']['host']
    kafka_port = configurator['clusters']['kafka']['port']
    st = StreamingPipeline(kafka_host, kafka_port, 'my_topic')
    st.start_streaming()
    logging.info("Hello Spark and Cassandra")


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