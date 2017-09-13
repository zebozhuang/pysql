# coding: utf-8

import time
import random
import unittest

from test.configreader import ConfigReader
from pool import SQLPool


config = ConfigReader.get('config.toml')


class TestInsert(unittest.TestCase):
    """
        Test pysql Insert API
    """
    TABLE_SCHEMA = """
        CREATE TABLE IF NOT EXISTS`t1`(
            id int(11) AUTO_INCREMENT,
            name varchar(32) DEFAULT '',
            age smallint DEFAULT '0',
            PRIMARY KEY(`id`),
            UNIQUE KEY `idx_name`(`name`)
        ) ENGINE=InnoDB default charset=utf8;
    """

    def setUp(self):
        self.pool = SQLPool(**config)

    def testUpdate(self):
        pass