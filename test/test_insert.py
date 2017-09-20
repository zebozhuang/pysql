# coding: utf-8

import time
import random
import unittest

from test.utils import get_test_name, get_test_age
from test.configreader import ConfigReader
from pool import SQLPool


config = ConfigReader.get('config.toml')


class TestInsert(unittest.TestCase):
    """
        Test pysql Insert API
    """
    TABLE = 't1'
    TABLE_SCHEMA = """
        CREATE TABLE IF NOT EXISTS`%s`(
            id int(11) AUTO_INCREMENT,
            name varchar(32) DEFAULT '',
            age smallint DEFAULT '0',
            PRIMARY KEY(`id`),
            UNIQUE KEY `idx_name`(`name`)
        ) ENGINE=InnoDB default charset=utf8;
    """ % TABLE

    def setUp(self):
        self.pool = SQLPool(**config)
        self.pool.execute(TestInsert.TABLE_SCHEMA)
        self.table = TestInsert.TABLE

    def tearDown(self):
        self.pool.delete(self.table)

    def _insertmany(self, objs, duplicate={}):
        return self.pool.insertmany(self.table, objs, dup=duplicate)

    def _insert(self, obj, duplicate={}):
        return self.pool.insert(self.table, obj, dup=duplicate)

    def testInsert(self):
        obj = {'name': get_test_name(), 'age': get_test_age()}
        insert_id = self._insert(obj=obj)

        assert insert_id is not None and insert_id > 0, "Wrong result: %s" % insert_id

    def testInsertmany(self):

        objs = [{'name': get_test_name(), 'age': get_test_age()} for _ in range(10)]
        insert_ids = self._insertmany(objs=objs)

        assert insert_ids is not None


if __name__ == '__main__':
    unittest.main()