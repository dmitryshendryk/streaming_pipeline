import os
import sys
import click
import logging
from typing import Dict, Optional, Tuple


PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
PROJECT_ROOT = os.path.join(PROJECT_ROOT, 'ihs')

sys.path.append(PROJECT_ROOT)



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