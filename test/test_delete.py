# coding: utf-8

import unittest

from test.configreader import ConfigReader
from test.utils import get_test_name
from pool import SQLPool


config = ConfigReader.get('config.toml')

class TestDelete(unittest.TestCase):
    """
        Test pysql Delete API
    """
    TABLE_SCHEMA = """
        CREATE TABLE IF NOT EXISTS`t3`(
            id int(11) AUTO_INCREMENT,
            name varchar(32) DEFAULT '',
            age smallint DEFAULT '0',
            PRIMARY KEY(`id`),
            UNIQUE KEY `idx_name`(`name`)
        ) ENGINE=InnoDB default charset=utf8;
    """

    def setUp(self):
        self.pool = SQLPool(**config)
        self.pool.execute(TestDelete.TABLE_SCHEMA)

    def _insertmany(self, objs):
        return self.pool.insertmany(table='t3', objs=objs)

    def testDelete(self):
        objs = [{'name': get_test_name()} for _ in range(10)]
        insert_ids = list(self._insertmany(objs=objs))

        assert len(insert_ids) == 10, 'Fail to insert data'

        where = {'id__in': insert_ids}
        affected_rows = self.pool.delete(table='t3', where=where)
        assert affected_rows == 10, 'Fail to delete data: %s' % affected_rows


if __name__ == '__main__':
    unittest.main()