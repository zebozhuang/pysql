# coding: utf-8

import unittest

from test.configreader import ConfigReader
from pool import SQLPool


config = ConfigReader.get('config.toml')


class TestTransaction(unittest.TestCase):
    """
        Test PySQL Transaciton.
    """
    TABLE = 't5'
    TABLE_SCHEMA = """
        CREATE TABLE IF NOT EXISTS`%s`(
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `name` varchar(32) DEFAULT '',
            `age` smallint DEFAULT '0',
            PRIMARY KEY(`id`),
            UNIQUE KEY `idx_name`(`name`)
        ) ENGINE=InnoDB default charset=utf8;
    """ % TABLE

    def setUp(self):
        self.pool = SQLPool(**config)
        self.pool.execute(TestTransaction.TABLE_SCHEMA)
        self.table = TestTransaction
    # def te
