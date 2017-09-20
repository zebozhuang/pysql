# coding: utf-8

import unittest

from test.configreader import ConfigReader
from test.utils import get_test_name
from pool import SQLPool


config = ConfigReader.get('config.toml')

class TestSelect(unittest.TestCase):
    """
        Test pysql Select API
    """
    TABLE = 't4'
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
        self.pool.execute(TestSelect.TABLE_SCHEMA)
        self.table = TestSelect.TABLE

    def _insertmany(self, objs):
        return self.pool.insertmany(table=self.table, objs=objs)

    def testDelete(self):
        objs = [{'name': get_test_name()} for _ in range(10)]
        insert_ids = list(self._insertmany(objs=objs))

        assert len(insert_ids) == 10, 'Fail to insert data'

        where = {'id__in': insert_ids}
        datas = list(self.pool.query(table=self.table, where=where))
        assert len(datas) == 10, 'Fail to select data: %s' % len(datas)


if __name__ == '__main__':
    unittest.main()