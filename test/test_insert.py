# coding: utf-8

import unittest

from test.configreader import ConfigReader
from pool import SQLPool


config = ConfigReader.get('config.toml')


class TestInsert(unittest.TestCase):
    """
        Test pysql Insert API
    """

    def setUp(self):
        self.pool = SQLPool(**config)

    def tearDown(self):
        pass
        # self.pool.insert()