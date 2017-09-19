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
        self.pool.execute(TestInsert.TABLE_SCHEMA)

    def testInsert(self):

        out = self.pool.insert(table='t1', obj={'name': 'name_%d' % int(time.time()), 'age': random.randint(1, 20)})
        assert out is not None and out > 0, "Wrong result: %s" % out

    def testInsertmany(self):

        objs = [{'age': random.randint(1, 20), 'name': 'name_%d_%d' % (int(time.time()*1000), random.randint(1, 10000))}
                for _ in range(10)]
        out = self.pool.insertmany(table='t1', objs=objs)
        # print(out)
        assert out is not None

    def tearDown(self):
        pass

if __name__ == '__main__':
    unittest.main()