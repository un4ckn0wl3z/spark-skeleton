import json
import os
from core.logger import log


class Configuration:
    def __init__(self, env):
        self.env = env
        self.file_dir = os.path.dirname(os.path.realpath('__file__'))
        self.file_path = '/config/%s-conf.json' % self.env
        self.full_path = self.file_dir + self.file_path

        log.info(f'Reading config from: {self.full_path}')

        with open(self.full_path) as config_file:
            self.data = json.load(config_file)

    @property
    def app_name(self):
        return self.data['app_name']

    @property
    def input_mongodb_uri(self):
        return self.data['input_mongodb_uri']

    @property
    def output_mongodb_uri(self):
        return self.data['output_mongodb_uri']

    @property
    def jars_dir(self):
        return self.data['jars_dir']

    @property
    def master(self):
        return self.data['master']
