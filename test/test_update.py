# coding: utf-8

import time
import random
import unittest

from test.configreader import ConfigReader
from test.utils import get_test_name, get_test_age
from pool import SQLPool



config = ConfigReader.get('config.toml')


class TestInsert(unittest.TestCase):
    """
        Test pysql Insert API
    """
    TABLE_SCHEMA = """
        CREATE TABLE IF NOT EXISTS`t2`(
            id int(11) AUTO_INCREMENT,
            name varchar(32) DEFAULT '',
            age smallint DEFAULT '0',
            PRIMARY KEY(`id`),
            UNIQUE KEY `idx_name`(`name`)
        ) ENGINE=InnoDB default charset=utf8;
    """

    def setUp(self):
        self.pool = SQLPool(**config)

    def _insert(self, obj):
        return self.pool.insert(table='t2', obj=obj)

    def _insertmany(self, objs):
        return self.pool.insertmany(table='t2', objs=objs)

    def testUpdate(self):
        # Case 1: update single data
        obj = {'age': random.randint(1, 30), 'name': 'name_%s_%s' % (int(time.time()*1000), random.randint(1, 1000))}
        insert_id = self._insert(obj)

        where = {'id': insert_id}
        obj = {'name': 'name_update_%s' % int(time.time()*1000)}
        affected_rows = self.pool.update(table='t2', where=where, obj=obj)
        assert affected_rows == 1, 'Fail to update: %d' % affected_rows

        # Case 2: update multiple data
        objs = []
        for i in range(10):
            objs.append(
                    {
                        'age': 1,
                        'name': get_test_name()
                    }
            )
        insert_ids = list(self._insertmany(objs))
        obj = {'age': 9}
        where = {'id__in': list(insert_ids)}
        affected_rows = self.pool.update(table='t2', where=where, obj=obj)
        assert affected_rows == 10, 'Fail to update: %d' % affected_rows

        # id > insert_ids[4]
        where = {'id__gt': insert_ids[4]}
        obj = {'age': 11}
        affected_rows = self.pool.update(table='t2', where=where, obj=obj)
        assert affected_rows == len(insert_ids[5:]), 'Fail to update: %d != %d' % (affected_rows, len(insert_ids[5:]))


        # id >= insert_ids[4]
        where = {'id__gte': insert_ids[4]}
        obj = {'age': 12}
        affected_rows = self.pool.update(table='t2', where=where, obj=obj)
        assert affected_rows == len(insert_ids[4:]), 'Fail to update: %d != %d' % (affected_rows, len(insert_ids[4:]))

        # id != insert_ids[4]
        where = {'id__neq': insert_ids[4]}
        obj = {'age': 13}
        affected_rows = self.pool.update(table='t2', where=where, obj=obj)
        assert affected_rows != 0, 'Fail to update'

        # id >= insert_ids[4] and id <= insert_ids[7]
        where = {'id__gte': insert_ids[4], 'id__lte': insert_ids[7]}
        obj = {'age': 14}
        affected_rows = self.pool.update(table='t2', where=where, obj=obj)
        assert affected_rows == 4, 'Fail to update: %d' % affected_rows


if __name__ == '__main__':
    unittest.main()
