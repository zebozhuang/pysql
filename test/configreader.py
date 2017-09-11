# coding: utf-8

import toml


class ConfigReader(object):

    @staticmethod
    def get(fpath):
        with open(fpath) as f:
            config = toml.loads(f.read())
            return config
