import yaml
import logging


class Configurator():

    def __init__(self, path=None):
        with open(path, "r") as configuration_file:
            try:
                self._configuration_data = yaml.load(configuration_file)
            except yaml.YAMLError as exc:
                logging.error(exc)