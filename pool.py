# coding: utf-8

import datetime
import threading


class ThreadDict(threading.local):
    """
    Thread local storage: solve the threading security of local variants.
        >>> d = ThreadDict()
        >>> d.x = 1
        >>> d.x
        1
        >>> import threading
        >>> def f(): d.x = 2
        ...
        >>> t = threading.Thread(target=f)
        >>> t.start()
        >>> t.join()
        >>> d.x
        1
    """

    _instances = set()

    def __init__(self):
        ThreadDict._instances.add(self)

    def __del__(self):
        ThreadDict._instances.remove(self)

    def __hash__(self):
        return id(self)

    def __getitem__(self, key):
        return self.__dict__[key]

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __delitem__(self, key):
        del self.__dict__[key]

    def __contains__(self, key):
        return key in self.__dict__

    def clear(self):
        self.__dict__.clear()

    def clear_all(self):
        """Clear all instances in ThreadDict"""
        for d in ThreadDict._instances:
            d.clear()

    def copy(self):
        return self.__dict__.copy()

    def get(self, key, default=None):
        return self.__dict__.get(key, default)

    def items(self):
        return self.__dict__.items()

    def iteritems(self):
        return iter(self.__dict__.items())

    def keys(self):
        return self.__dict__.keys()

    def iterkeys(self):
        return iter(self.__dict__.keys())

    iter = iterkeys

    def values(self):
        return self.__dict__.values()

    def itervalues(self):
        return iter(self.__dict__.values())

    def pop(self, key, *args):
        return self.__dict__.pop(key, *args)

    def popitem(self):
        return self.__dict__.popitem()

    def setdefault(self, key, default=None):
        return self.__dict__.setdefault(key, default)

    def update(self, *args, **kwargs):
        self.__dict__.update(*args, **kwargs)

    def __repr__(self):
        return '<ThreadDict %s>' % self.__dict__

    __str__ = __repr__


class Item(dict):
    """
    A Item object is like a dictionary which can not only be used as `obj.foo`,
    but also be used as `obj['foo']`.

        >>> item = Item(x=1)
        >>> item.x
        1
        >>> item['x']
        1
        >>> item.x = 2
        >>> item['x']
        2
        >>> del item.x
        >>> item.x
        Traceback (most recent call last):
        ...
        AttributeError: x
    """

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(e)

    def __setattr__(self, key, value):
        self[key] = value

    def __delitem__(self, key):
        try:
            del self[key]
        except KeyError as e:
            raise AttributeError(e)

    def __repr__(self):
        return '<Item ' + dict.__repr__(self) + '>'


class SQLParam(object):
    """
    Parameter in SQLQuery
    """

    __slots__ = ['value']

    def __init__(self, value):
        self.value = value

    def get_marker(self):
        return '%s'

    def __add__(self, other):
        return self.sqlquery() + other

    def __radd__(self, other):
        return other + self.sqlquery()

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return '<param: %s>' % repr(self.value)

    def sqlquery(self):
        return SQLQuery([self])


class SQLQuery(object):
    """
        Compose SQL with parameters.
    """

    __slots__ = ['items']

    def __init__(self, items=None):
        if items is None:
            self.items = []
        elif isinstance(items, list):
            self.items = items
        elif isinstance(items, SQLParam):
            self.items = [items]
        elif isinstance(items, SQLQuery):
            self.items = items.items
        else:
            self.items = [items]

    def __add__(self, other):
        if isinstance(other, str):
            items = [other]
        elif isinstance(other, SQLQuery):
            items = other.items
        else:
            raise NotImplemented
        return SQLQuery(self.items + items)

    def __radd__(self, other):
        if isinstance(other, str):
            items = [other]
        else:
            raise Exception("NotImplement")
        return SQLQuery(items + self.items)

    def __iadd__(self, other):
        if isinstance(other, (str, SQLParam)):
            self.items.append(other)
        elif isinstance(other, SQLQuery):
            self.items.extend(other.items)
        else:
            raise Exception("NotImplement")
        return self

    def __len__(self):
        return len(self.query())

    def __str__(self):
        try:
            return self.query() % tuple([sqlify(obj) for obj in self.values()])
        except (ValueError, TypeError):
            return self.query()

    def __repr__(self):
        return '<sql: %s>' % repr(str(self))

    def query(self):
        """
        Returns the query part of the sql query.
            >>> q = SQLQuery(["SELECT * FROM test WHERE name=", SQLParam('joe')])
            >>> q.query()
            'SELECT * FROM test WHERE name=%s'
            >>> q.query(paramstyle='qmark')
            'SELECT * FROM test WHERE name=?'
        """
        s = []
        for x in self.items:
            if isinstance(x, SQLParam):
                s.append(x.get_marker())
            else:
                x = str(x)
                if '%%' in x:
                    x = x.replace('%', '%%')
                s.append(x)
        return ''.join(s)

    def values(self):
        return [i.value for i in self.items if isinstance(i, SQLParam)]

    def join(items, sep=' ', prefix=None, suffix=None, target=None):
        """
        Joins multiple queries.

            >>> SQLQuery.join(['a', 'b'], ', ')
            <sql: 'a, b'>

        Optinally, prefix and suffix arguments can be provided.
            >>> SQLQuery.join(['a', 'b'], ', ', prefix='(', suffix=')')
            <sql: '(a, b)'>
        If target argument is provided, the items are appended to target instead of creating a new SQLQuery.
        """
        if target is None:
            target = SQLQuery()

        if prefix:
            target.items.append(prefix)
        for i, item in enumerate(items):
            if i != 0:
                target.items.append(sep)
            target.items.append(item)
        if suffix:
            target.items.append(suffix)
        return target


class Field(object):
    """
    Field is to combine keys and constants together like `num=Field('num+1')` etc.
    """
    def __init__(self, s):
        self.s = s

    def __str__(self):
        return str(self.s)


def sqlconvert(data, grouping=', '):
    """
    Convert a two tuple (key, value) iterable `data` to an SQL key=value with a grouping.
        :param data: [(key1, value1), (key2, value2), ...]
        :param grouping: Separate key/values pairs(`,` and `and`).
        :return: SQLQuery
    """

    items = []
    for k, v in data:
        if isinstance(v, Field):
            # update table set value=Field(value+1)
            items.append("".join((k, ' = ', str(v))))
        else:
            items.append("".join((k, ' = ', SQLParam(v))))
    return SQLQuery.join(items, grouping)


def sqlify(obj):
    """
    Convert `obj` to its proper SQL version
    :param obj:
    :return:
    """
    if obj is None:
        return 'NULL'
    elif isinstance(obj, int):
        return str(obj)
    elif isinstance(obj, datetime.datetime):
        return repr(obj.isoformat())
    else:
        return repr(obj)

_SQL_OPERATOR = {
    'GT': ' > ',
    'GTE': ' >= ',
    'EQ': ' = ',
    'NEQ': ' != ',
    'LT': ' < ',
    'LTE': ' <= ',
    'IN': ' IN ',
    'NIN': ' NOT IN ',
    'LIKE': ' LIKE ',
    'REGEXP': ' REGEXP ',
    'SQL': ' SQL ',
}

def sqloperator(operator):
    """
    Translate operator into sql operator.
    """
    operator = operator.upper()
    assert operator in _SQL_OPERATOR, 'Not support operator: %s' % str(operator)

    return _SQL_OPERATOR[operator]


class DB(object):
    """Basic MySQL CRUD API"""

    def __init__(self, module, config):
        """
        :param
            module: Module is the package like pymysql, MySQLdb etc.
                    Create and connect to database server in local function.
                    Wrap the DB Api to handle all kinds of Databases.
            config: Config is the configuration of Database(host,passwd,port etc).
        """
        self.module = module
        self.config = config
        self._ctx = ThreadDict()

    def _getctx(self):
        if not self._ctx.get('db'):
            self._load_context(self._ctx)
        return self._ctx

    ctx = property(_getctx)

    def _load_context(self, ctx):
        """Create a db connection"""
        ctx.dbq_count = 0
        ctx.transactions = []
        ctx.db = self._connect(self.config)

        def commit(unload=True):
            ctx.db.commit()
            if unload:
                self._unload_context(self._ctx)

        def rollback():
            ctx.db.rollback()
            self._unload_context(self._ctx)

        ctx.commit = commit
        ctx.rollback = rollback

    def _unload_context(self, ctx):
        """Delete db connection, the connection will recycle in PoolDB"""
        del ctx.db

    def _connect(self, config):
        """Connect to db server, and return a connection."""
        def get_pooled_db():
            from DBUtils import PooledDB
            # Please configure mincached, maxchached, maxshared in config.
            return PooledDB.PooledDB(creator=self.module, **config)

        if getattr(self, '_pooleddb', None) is None:
            self._pooleddb = get_pooled_db()
        return self._pooleddb.connection()

    def _db_cursor(self):
        """Get cursor from ctx.db"""
        return self.ctx.db.cursor()

    def _db_execute(self, cursor, sqlquery):
        """Execute a real sql query"""
        self.ctx.dbq_count += 1
        try:
            query, params = self._process_query(sqlquery)
            out = cursor.execute(query, params)
        except Exception as e:
            if self.ctx.transactions:
                self.ctx.transactions[-1].rollback()
            else:
                self.ctx.rollback()
            raise e
        return out

    def _process_query(self, sqlquery):
        """Separate sql and params from sqlquery"""
        print(sqlquery)
        query = sqlquery.query()
        params = sqlquery.values() or None  # None because of api `execute(sql, params=None)`
        return query, params

    def insert(self, table, obj={}, mode='INSERT', dup={}):
        """
        Insert data into table with INSERT, REPLACE and INSERT_IGNORE mode.
        :param
            table: table name
            obj: data inserted to table
            mode: INSERT, REPLACE, INSERT_IGNORE mode
            dup: duplicate if data exists.
        """
        mode = mode.upper()
        assert mode in ('INSERT', 'REPLACE', 'INSERT_IGNORE'), 'Wrong insert mode: %s' % mode

        kvs = sorted(obj.items(), key=lambda v: v[0])
        keys = SQLQuery.join(map(lambda kv: kv[0], kvs), sep=', ', prefix='(', suffix=')')
        values = SQLQuery.join(map(lambda kv: SQLParam(kv[1]), kvs), sep=', ', prefix='(', suffix=')')

        sqlquery = "%s INTO %s " % (mode, table) + keys + " VALUES " + values
        if dup:
            sqlquery += " ON DUPLICATE KEY UPDATE %s" % sqlconvert(dup.items())

        cursor = self._db_cursor()
        self._db_execute(cursor, sqlquery)
        self._db_execute(cursor, SQLQuery('SELECT last_insert_id()'))

        try:
            out = cursor.fetchone()[0]
        except Exception as e:
            # TODO: error log
            out = None

        if not self.ctx.transactions:
            self.ctx.commit()
        return out


class Transaction(object):
    """
    Database Transaction: wrap ctx and commit transactions
    """

    def __init__(self, ctx):
        self.ctx = ctx
        self.transaction_count = transaction_count = len(ctx.transactions)

        class TransactionEngine(object):
            """Transaction Engine used in top level transaction"""
            def do_transact(self):
                ctx.commit(unload=False)

            def do_commit(self):
                ctx.commit()

            def do_rollback(self):
                ctx.rollback()


        class SubtransactionEngine(object):
            """
                Transaction Engine used in sub transaction.
            """
            def query(self, q):
                cursor = ctx.db.cursor()
                ctx.db_execute(cursor, SQLQuery(q % transaction_count))

            def do_transact(self):
                self.query('SAVEPOINT point_%s')

            def do_commit(self):
                self.query('RELEASE SAVEPOINT point_%s')

            def do_rollback(self):
                self.query('ROLLBACK TO SAVEPIONT sp_%s')


        class DummyEngine(object):
            """
                Transaction Engine used instead of SubtransactionEngine
                when subtransaction are not supported. MySQL supports
                subtransaciton. DummyEngine is used for another DB of which
                subtransaction is not supported.
            """
            do_transact = do_commit = do_rollback = lambda self: None

        if self.transaction_count:
            if self.ctx.get('ignore_nested_transaction'):
                self.engine = DummyEngine()
            else:
                self.engine = SubtransactionEngine()
        else:
            self.engine = TransactionEngine()

        self.engine.do_transact()
        self.ctx.transactions.append(self)


class SQLPool(DB):
    """
    MySQL Pool Connection
    """

    def __init__(self, **config):
        module = import_driver(['pymysql', 'MySQLdb'])

        config['port'] = int(config['port'])
        config['charset'] = config.get('charset', 'utf8')

        super(SQLPool, self).__init__(module, config)

        self.support_multiple_insert = True



def import_driver(drivers):
    """
    Import the first available driver
    """
    for driver in drivers:
        try:
            return __import__(driver)
        except ImportError:
            pass
    raise ImportError('Unable to import ' + ' or '.join(drivers))



if __name__ == '__main__':
    import doctest
    doctest.testmod()