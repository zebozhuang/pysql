import logging
import time
import datetime
import threading
import re
from DBUtils import PooledDB

# from gevent import monkey
# monkey.patch_all()


# logging.basicConfig(level=logging.DEBUG)
# LOGGER = logging.getLogger("sqlpool")

TOKEN = '[ \\f\\t]*(\\\\\\r?\\n[ \\f\\t]*)*(#[^\\r\\n]*)?(((\\d+[jJ]|((\\d+\\.\\d*|\\.\\d+)([eE][-+]?\\d+)?|\\d+[eE][-+]?\\d+)[jJ])|((\\d+\\.\\d*|\\.\\d+)([eE][-+]?\\d+)?|\\d+[eE][-+]?\\d+)|(0[xX][\\da-fA-F]+[lL]?|0[bB][01]+[lL]?|(0[oO][0-7]+)|(0[0-7]*)[lL]?|[1-9]\\d*[lL]?))|((\\*\\*=?|>>=?|<<=?|<>|!=|//=?|[+\\-*/%&|^=<>]=?|~)|[][(){}]|(\\r?\\n|[:;.,`@]))|([uUbB]?[rR]?\'[^\\n\'\\\\]*(?:\\\\.[^\\n\'\\\\]*)*\'|[uUbB]?[rR]?"[^\\n"\\\\]*(?:\\\\.[^\\n"\\\\]*)*")|[a-zA-Z_]\\w*)'

tokenprog = re.compile(TOKEN)

OPERATOR = {
    'gt': ' > ',
    'gte': ' >= ',
    'eq': ' = ',
    'neq': ' != ',
    'lt': ' < ',
    'lte': ' <= ',
    'in': ' IN ',
    'nin': ' NOT IN ',
    'like': ' LIKE ',
    'regexp': ' REGEXP ',
    'sql': ' SQL ',
}


class _ItplError(ValueError):
    def __init__(self, text, pos):
        ValueError.__init__(self)
        self.text = text
        self.pos = pos

    def __str__(self):
        return "unfinished expression in %s at char %d" % (
            repr(self.text), self.pos
        )

class Field(object):
    def __init__(self, s):
        self.s = s

    def __str__(self):
        return str(self.s)


class ThreadedDict(threading.local):
    """
    Thread local storage.

        >>> d = ThreadedDict()
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

    _instance = set()

    def __init__(self):
        ThreadedDict._instance.add(self)

    def __del__(self):
        ThreadedDict._instance.remove(self)

    def __hash__(self):
        return id(self)

    def clear_all():
        """Clear all ThreadDict instance"""
        for t in list(ThreadedDict._instance):
            t.clear()

    clear_all = staticmethod(clear_all)

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
        return '<ThreadedDict %s>' % self.__dict__

    __str__ = __repr__

class Storage(dict):
    """
    A Storage object is like a dictionary except `obj.foo` can be used
    in addition to `obj['foo']`.

        >>> o = storage(a=1)
        >>> o.a
        1
        >>> o['a']
        1
        >>> o.a = 2
        >>> o['a']
        2
        >>> del o.a
        >>> o.a
        Traceback (most recent call last):
            ...
        AttributeError: 'a'

    """
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as k:
            raise AttributeError(k)

    def __setattr__(self, key, value):
        self[key] = value

    def __delattr__(self, key):
        try:
            del self[key]
        except KeyError as k:
            raise AttributeError(k)

    def __repr__(self):
        return '<Storage ' + dict.__repr__(self) + '>'


class TransactionError(Exception): pass


class IterBetter(object):
    """
    Returns an object that can be used as an iterator
    but can also be used via __getitem__ (although it
    cannot go backwards -- that is, you cannot request
    `iterbetter[0]` after requesting `iterbetter[1]`).

        >>> import itertools
        >>> c = iterbetter(itertools.count())
        >>> c[1]
        1
        >>> c[5]
        5
        >>> c[3]
        Traceback (most recent call last):
            ...
        IndexError: already passed 3
    It is also possible to get the first value of the iterator or None.
        >>> c = iterbetter(iter([3, 4, 5]))
        >>> print(c.first())
        3
        >>> c = iterbetter(iter([]))
        >>> print(c.first())
        None
    For boolean test, IterBetter peeps at first value in the itertor without effecting the iteration.
        >>> c = iterbetter(iter(range(5)))
        >>> bool(c)
        True
        >>> list(c)
        [0, 1, 2, 3, 4]
        >>> c = iterbetter(iter([]))
        >>> bool(c)
        False
        >>> list(c)
        []
    """

    def __init__(self, iterator):
        self.i, self.c = iterator, 0

    def first(self, default=None):
        """Returns the first element of the iterator or None when there are no
        elements.
        If the optional argument default is specified, that is returned instead
        of None when there are no elements.
        """
        try:
            return next(iter(self))
        except StopIteration:
            return default

    def __iter__(self):
        if hasattr(self, "_head"):
            yield self._head

        while 1:
            yield next(self.i)
            self.c += 1

    def __getitem__(self, i):
        # todo: slices
        if i < self.c:
            raise IndexError("already passed " + str(i))

        try:
            while i > self.c:
                next(self.i)
                self.c += 1
            self.c += 1
            return next(self.i)
        except StopIteration:
            raise IndexError(str(i))

    def __nonzero__(self):
        if hasattr(self, '__len__'):
            return self.__len__() != 0
        elif hasattr(self, '_head'):
            return True
        else:
            try:
                self._head = next(self.i)
            except StopIteration:
                return False
            else:
                return True

    __bool__ = __nonzero__


class SQLParam(object):
    """
    Parameter in SQLQuery
    """
    __slots__ = ["value"]

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
    __slots__ = ["items"]

    def __init__(self, items=None):
        if items is None:
            self.items = []
        elif isinstance(items, list):
            self.items = items
        elif isinstance(items, SQLParam):
            self.items = [items]
        elif isinstance(items, SQLQuery):
            self.items = list(items.items)
        else:
            self.items = [items]

        for i, item in enumerate(self.items):
            if isinstance(item, SQLParam) and isinstance(item.value, SQLLiteral):
                self.items[i] = item.value.v

    def append(self, value):
        self.items.append(value)

    def __add__(self, other):
        if isinstance(other, str):
            items = [other]
        elif isinstance(other, SQLQuery):
            items = other.items
        else:
            return NotImplemented
        return SQLQuery(self.items + items)

    def __radd__(self, other):
        if isinstance(other, str):
            items = [other]
        else:
            return NotImplemented

        return SQLQuery(items + self.items)

    def __iadd__(self, other):
        if isinstance(other, (str, SQLParam)):
            self.items.append(other)
        elif isinstance(other, SQLQuery):
            self.items.extend(other.items)
        else:
            return NotImplemented
        return self

    def __len__(self):
        return len(self.query())

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
                if '%' in x and '%%' in x:
                    x = x.replace('%', '%%')
                s.append(x)
        return "".join(s)

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

        target_items = target.items
        if prefix:
            target_items.append(prefix)
        for i, item in enumerate(items):
            if i != 0:
                target_items.append(sep)
            if isinstance(item, SQLQuery):
                target_items.extend(item.items)
            else:
                target_items.append(item)
        if suffix:
            target_items.append(suffix)
        return target

    join = staticmethod(join)

    def _str(self):
        try:
            return self.query() % tuple([_sqlify(x) for x in self.values()])
        except (ValueError, TypeError):
            return self.query()

    def __str__(self):
        return str(self._str())

    def __unicode__(self):
        return str(self._str())

    def __repr__(self):
        return '<sql: %s>' % repr(str(self))


class SQLLiteral:
    """
    Protects a string from `sqlquote`.
        >>> sqlquote('NOW()')
        <sql: "'NOW()'">
        >>> sqlquote(SQLLiteral('NOW()'))
        <sql: 'NOW()'>
    """

    def __init__(self, v):
        self.v = v

    def __repr__(self):
        return self.v


def _reparam(string_, dictionary):
    """
    Takes a string and a dictionary and interpolates the string
    using values from the dictionary. Returns an `SQLQuery` for the result.
        >>> reparam("s = $s", dict(s=True))
        <sql: "s = 't'">
        >>> reparam("s IN $s", dict(s=[1, 2]))
        <sql: 's IN (1, 2)'>
    """

    dictionary = dictionary.copy()
    # distable builtins to avoid risk for remote code execution.

    dictionary['__builtins__'] = object()
    vals = []
    result = []
    for live, chunk in _interpolate(string_):
        if live:
            v = eval(chunk, dictionary)
            result.append(_sqlquote(v))
        else:
            result.append(chunk)
    return SQLQuery.join(result, '')


def _interpolate(format):
    """
    Takes a format string and returns a list of 2-tuples of the form
    (boolean, string) where boolean says whether string should be evaled
    or not.
    from <http://lfw.org/python/Itpl.py> (public domain, Ka-Ping Yee)
    """

    def matchorfail(text, pos):
        match = tokenprog.match(text, pos)
        if match is None:
            raise _ItplError(text, pos)
        return match, match.end()

    namechars = "abcdefghijklmnopqrstuvwxyz" \
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
    chunks = []
    pos = 0

    while 1:
        dollar = format.find("$", pos)
        if dollar < 0:
            break
        nextchar = format[dollar + 1]

        if nextchar == "{":
            chunks.append((0, format[pos:dollar]))
            pos, level = dollar + 2, 1
            while level:
                match, pos = matchorfail(format, pos)
                tstart, tend = match.regs[3]
                token = format[tstart:tend]
                if token == "{":
                    level = level + 1
                elif token == "}":
                    level = level - 1
            chunks.append((1, format[dollar + 2:pos - 1]))

        elif nextchar in namechars:
            chunks.append((0, format[pos:dollar]))
            match, pos = matchorfail(format, dollar + 1)
            while pos < len(format):
                if format[pos] == "." and \
                                        pos + 1 < len(format) and format[pos + 1] in namechars:
                    match, pos = matchorfail(format, pos + 1)
                elif format[pos] in "([":
                    pos, level = pos + 1, 1
                    while level:
                        match, pos = matchorfail(format, pos)
                        tstart, tend = match.regs[3]
                        token = format[tstart:tend]
                        if token[0] in "([":
                            level = level + 1
                        elif token[0] in ")]":
                            level = level - 1
                else:
                    break
            chunks.append((1, format[dollar + 1:pos]))
        else:
            chunks.append((0, format[pos:dollar + 1]))
            pos = dollar + 1 + (nextchar == "$")

    if pos < len(format):
        chunks.append((0, format[pos:]))
    return chunks


def _sqlquote(a):
    """
    Ensures `a` is quoted properly for use in a SQL query.
        >>> 'WHERE x = ' + sqlquote(True) + ' AND y = ' + sqlquote(3)
        <sql: "WHERE x = 't' AND y = 3">
        >>> 'WHERE x = ' + sqlquote(True) + ' AND y IN ' + sqlquote([2, 3])
        <sql: "WHERE x = 't' AND y IN (2, 3)">
    """
    if isinstance(a, list):
        return _sqllist(a)
    else:
        return SQLParam(a).sqlquery()


# def sqlors(left, lst):
#     """
#     `left is a SQL clause like `tablename.arg = `
#     and `lst` is a list of values. Returns a reparam-style
#     pair featuring the SQL that ORs together the clause
#     for each item in the lst.
#         >>> sqlors('foo = ', [])
#         <sql: '1=2'>
#         >>> sqlors('foo = ', [1])
#         <sql: 'foo = 1'>
#         >>> sqlors('foo = ', 1)
#         <sql: 'foo = 1'>
#         >>> sqlors('foo = ', [1,2,3])
#         <sql: '(foo = 1 OR foo = 2 OR foo = 3 OR 1=2)'>
#     """
#     if isinstance(lst, (list, tuple, set, frozenset)):
#         lst = list(lst)
#         ln = len(lst)
#         if ln == 0:
#             return SQLQuery("1=2")
#         if ln == 1:
#             lst = lst[0]
#
#     if isinstance(lst, (list, tuple, set, frozenset)):
#         return SQLQuery(['('] + sum([[left, SQLParam(x), ' OR '] for x in lst], []) + ["1=2)"])
#     else:
#         return left + SQLParam(lst)


def sqlor(conds=[]):
    where = []
    for item in conds:
        if isinstance(item, dict):
            c = []
            for k in item:
                v = item[k]
                k = k.split("__")
                k, operator = (k[0], k[1]) if len(k) == 2 else (k[0], 'EQ')
                c.append(k + sqloperator(operator) + _sqlquote(v))
            where.append(SQLQuery.join(c, " AND "))
        elif isinstance(item, SQLQuery):
            where.append(item)
        else:
            raise Exception("Unknow Data Type: %s" % str(item))
    if where:
        return SQLQuery.join(where, " OR ", prefix="(", suffix=")")
    return None


def _sqlwhere(data, grouping=' AND '):
    """
    Converts a two-tuple (key, value) iterable `data` to an SQL WHERE clause `SQLQuery`.

        >>> sqlwhere((('cust_id', 2), ('order_id',3)))
        <sql: 'cust_id = 2 AND order_id = 3'>
        >>> sqlwhere((('order_id', 3), ('cust_id', 2)), grouping=', ')
        <sql: 'order_id = 3, cust_id = 2'>
        >>> sqlwhere((('a', 'a'), ('b', 'b'))).query()
        'a = %s AND b = %s'
    """
    items = []
    for k, v in data:
        if isinstance(v, Field):        # update table set value=Field(value+1)
            items.append(k + ' = ' + str(v))
        else:
            items.append(k + ' = ' + SQLParam(v))

    return SQLQuery.join(items, grouping)


def _sqlify(obj):
    """
    converts `obj` to its proper SQL version
        >>> sqlify(None)
        'NULL'
        >>> sqlify(True)
        "'t'"
        >>> sqlify(3)
        '3'
    """
    if obj is None:
        return 'NULL'
    elif obj is True:
        return "'t'"
    elif obj is False:
        return "'f'"
    elif isinstance(obj, int):
        return str(obj)
    elif isinstance(obj, datetime.datetime):
        return repr(obj.isoformat())
    else:
        return repr(obj)


def _sqllist(values):
    """
        >>> _sqllist([1, 2, 3])
        <sql: '(1, 2, 3)'>
    """
    items = []
    items.append('(')
    for i, v in enumerate(values):
        if i != 0:
            items.append(', ')
        items.append(SQLParam(v))
    items.append(')')
    return SQLQuery(items)


def sqllist(lst):
    """
    Converts the arguments for use in something like a WHERE clause.

        >>> sqllist(['a', 'b'])
        'a, b'
        >>> sqllist('a')
        'a'
    """
    if isinstance(lst, str):
        return lst
    else:
        return ', '.join(lst)


def sqloperator(op):
    try:
        op = op.lower()
        assert op in OPERATOR
    except AssertionError:
        raise Exception("not supported operator: %s" % op)
    return OPERATOR[op]

class Transaction:
    """Database transaction."""

    def __init__(self, ctx):
        self.ctx = ctx
        self.transaction_count = transaction_count = len(ctx.transactions)

        class transaction_engine:
            """Transaction Engine used in top level transaction"""
            def do_transact(self):
                ctx.commit(unload=False)

            def do_commit(self):
                ctx.commit()

            def do_rollback(self):
                ctx.rollback()

        class subtransaction_engine:
            """Transaction Engine used in sub transactions"""

            def query(self, q):
                db_cursor = ctx.db.cursor()
                ctx.db_execute(db_cursor, SQLQuery(q % transaction_count))

            def do_transact(self):
                self.query('SAVEPOINT webpy_sp_%s')

            def do_commit(self):
                self.query('RELEASE SAVEPOINT webpy_sp_%s')

            def do_rollback(self):
                self.query('ROLLBACK TO SAVEPOINT webpy_sp_%s')

        class dummy_engine:
            """Transaction Engine used instead of subtransaction_engine
            when sub transactions are not supported."""
            do_transaction = do_commit = do_rollback = lambda self: None

        if self.transaction_count:
            if self.ctx.get('ignore_nested_transaction'):
                self.engine = dummy_engine()
            else:
                self.engine = subtransaction_engine()
        else:
            self.engine = transaction_engine()

        self.engine.do_transact()
        self.ctx.transactions.append(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.rollback()
        else:
            self.commit()

    def commit(self):
        if len(self.ctx.transactions) > self.transaction_count:
            self.engine.do_commit()
            self.ctx.transactions = self.ctx.transactions[:self.transaction_count]

    def rollback(self):
        if len(self.ctx.transactions) > self.transaction_count:
            self.engine.do_rollback()
            self.ctx.transactions = self.ctx.transactions[:self.transaction_count]

class DB(object):
    def __init__(self, db_module, config):
        self.db_module = db_module
        self.config = config
        self._ctx = ThreadedDict()
        self.printing = True

    def _getctx(self):
        if not self._ctx.get("db"):
            self._load_context(self._ctx)
        return self._ctx

    ctx = property(_getctx)

    def _load_context(self, ctx):
        ctx.dbq_count = 0
        ctx.transactions = []
        ctx.db = self._connect(self.config)
        ctx.db_execute = self._db_execute
        if not hasattr(ctx.db, 'commit'):
            ctx.db.commit = lambda: None
        if not hasattr(ctx.db, 'rollback'):
            ctx.db.rollback = lambda: None

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
        del ctx.db

    def _connect(self, config):
        def get_pooled_db():
            return PooledDB.PooledDB(creator=self.db_module, mincached=3, maxcached=3, maxshared=0, **config)

        if getattr(self, '_pooleddb', None) is None:
            self._pooleddb = get_pooled_db()

        return self._pooleddb.connection()

    def _db_cursor(self):
        return self.ctx.db.cursor()

    def test_execute(self, sql, param):
        db_cursor = self._db_cursor()
        db_cursor.execute(sql, param)
        self.ctx.commit()

    def _db_execute(self, cur, sql_query, params=None):
        """execute an sql query"""
        self.ctx.dbq_count += 1
        try:
            a = time.time()
            query, params = self._process_query(sql_query)
            # print(query)
            if not params:
                params = None
            # print(params)
            # if params:
            #     print(query % tuple(params))
            out = cur.execute(query, params)
            b = time.time()
        except Exception as e:
            print(e)
            if self.ctx.transactions:
                self.ctx.transactions[-1].rollback()
            else:
                self.ctx.rollback()
            raise

        return out

    def _process_query(self, sql_query):
        query = sql_query.query()
        params = sql_query.values()
        return query, params

    def _where(self, where, vars):
        if isinstance(where, dict):
            where = self._where_dict(where)
        elif isinstance(where, SQLQuery):
            pass
        else:
            where = _reparam(where, vars)
        return where

    def _where_dict(self, where):
        where_clauses = []
        for k, v in sorted(where.items(), key=lambda t: t[0]):
            _k = k.split("__")
            k, op = (_k[0], _k[1]) if len(_k) == 2 else (_k[0], 'EQ')
            where_clauses.append(k + sqloperator(op) + _sqlquote(v))
        if where_clauses:
            return SQLQuery.join(where_clauses, " AND ")
        else:
            return None

    def insert(self, table_name, obj_dict, mode='INSERT', duplicate={}, print_sql=False):
        try:
            mode = mode.upper()
            assert mode in ("INSERT", "REPLACE", "INSERT_IGNORE")
        except AssertionError:
            raise

        def q(x):
            return "(" + x + ")"

        sorted_values = sorted(obj_dict.items(), key=lambda t: t[0])
        _keys = SQLQuery.join(map(lambda t: t[0], sorted_values), ', ')
        _values = SQLQuery.join([SQLParam(v) for v in map(lambda t: t[1], sorted_values)], sep=', ')

        print(_values)
        sql_query = "%s INTO %s " % (mode, table_name) + q(_keys) + " VALUES " + q(_values)

        if duplicate:
            sql_query += "ON DUPLICATE KEY UPDATE %s" % _sqlwhere(duplicate.items(), ", ")

        db_cursor = self._db_cursor()

        sql_query = self._process_insert_query(sql_query, table_name, seqname='')

        print(sql_query)
        if isinstance(sql_query, tuple):
            q1, q2 = sql_query
            self._db_execute(db_cursor, q1)
            self._db_execute(db_cursor, q2)
        else:
            self._db_execute(db_cursor, sql_query)

        try:
            out = db_cursor.fetchone()[0]
        except Exception as e:
            out = None

        if not self.ctx.transactions:
            self.ctx.commit()
        return out

    def _process_insert_query(self, query, tablename, seqname):
        return query

    def multiple_insert(self, table_name, obj_list, mode='INSERT', duplicate={}, print_sql=False):

        try:
            assert mode in ("INSERT", "REPLACE", "INSERT_IGNORE")
        except AssertionError:
            raise

        if not obj_list:
            return []

        if not self.supports_multiple_insert:
            out = [self.insert(table_name, obj) for obj in obj_list]
            return out

        keys = obj_list[0].keys()

        for v in obj_list:

            if v.keys() != keys:
                raise ValueError("Not all rows have the same keys")

        keys = sorted(keys)

        sql_query = SQLQuery("%s INTO %s (%s) VALUES " % (mode, table_name, ", ".join(keys)))
        for i, row in enumerate(obj_list):
            if i != 0:
                sql_query.append(", ")
            SQLQuery.join([SQLParam(row[k]) for k in keys], sep=", ", target=sql_query, prefix="(", suffix=")")

        if duplicate:
            sql_query.append("ON DUPLICATE KEY UPDATE %s" % _sqlwhere(duplicate.items(), ", "))

        db_cursor = self._db_cursor()
        sql_query = self._process_insert_query(sql_query, table_name, '')
        if isinstance(sql_query, tuple):
            q1, q2 = sql_query
            self._db_execute(db_cursor, q1)
            self._db_execute(db_cursor, q2)
        else:
            self._db_execute(db_cursor, sql_query)

        try:
            out = db_cursor.fetchone()[0]
            out = range(out, out + len(obj_list))
        except Exception as e:
            out = None

        if not self.ctx.transactions:
            self.ctx.commit()
        return out

    def update(self, table_name, where, update_dict, print_sql=False):
        """table name 可以是多个"""
        """
        Update `tables` with clause `where` (interpolated using `vars`)
        and setting `values`.
            >>> db = DB(None, {})
            >>> name = 'Joseph'
            >>> q = db.update('foo', where='name = $name', name='bob', age=2,
            ...     created=SQLLiteral('NOW()'), vars=locals(), _test=True)
            >>> q
            <sql: "UPDATE foo SET age = 2, created = NOW(), name = 'bob' WHERE name = 'Joseph'">
            >>> q.query()
            'UPDATE foo SET age = %s, created = NOW(), name = %s WHERE name = %s'
            >>> q.values()
            [2, 'bob', 'Joseph']
        """
        where = self._where(where, {})

        values = sorted(update_dict.items(), key=lambda t: t[0])
        query = (
            "UPDATE " + sqllist(table_name) +
            " SET " + _sqlwhere(values, ', ')
        )
        if where:
            query += (" WHERE " + where)
        db_cursor = self._db_cursor()
        self._db_execute(db_cursor, query)
        if not self.ctx.transactions:
            self.ctx.commit()
        return db_cursor.rowcount

    def delete(self, table_name, where={}, using=None, print_sql=False):
        """
        Deletes from `table` with clauses `where` and `using`.
            >>> db = DB(None, {})
            >>> name = 'Joe'
            >>> db.delete('foo', where='name = $name', vars=locals(), _test=True)
            <sql: "DELETE FROM foo WHERE name = 'Joe'">
        """
        where = self._where(where, {})
        q = "DELETE FROM " + table_name
        if using: q += ' USING ' + sqllist(using)
        if where: q += ' WHERE ' + where

        if not isinstance(q, SQLQuery):
            q = SQLQuery(q)

        db_cursor = self._db_cursor()
        self._db_execute(db_cursor, q)
        if not self.ctx.transactions:
            self.ctx.commit()
        return db_cursor.rowcount

    def query(self, table_name, where={}, fields=['*'], page=0, page_num=50, group_by=None,
              having=None, order_by=None, print_sql=False):
        """
        Selects `what` from `tables` with clauses `where`, `order`,
        `group`, `limit`, and `offset`. Uses vars to interpolate.
        Otherwise, each clause can be a SQLQuery.

            >>> db = DB(None, {})
            >>> db.select('foo', _test=True)
            <sql: 'SELECT * FROM foo'>
            >>> db.select(['foo', 'bar'], where="foo.bar_id = bar.id", limit=5, _test=True)
            <sql: 'SELECT * FROM foo, bar WHERE foo.bar_id = bar.id LIMIT 5'>
            >>> db.select('foo', where={'id': 5}, _test=True)
            <sql: 'SELECT * FROM foo WHERE id = 5'>
        """
        page, page_num = map(int, (page, page_num))
        if page == 0:
            limit = None
            offset = None
        else:
            limit = page_num
            offset = (page-1)*page_num

        fields = ",".join(fields)
        sql_clauses = self.sql_clauses(fields, table_name, where, group_by, having, order_by, limit, offset)

        clauses = [self.gen_clause(sql, val, {}) for sql, val in sql_clauses if val is not None]
        qout = SQLQuery.join(clauses)
        return self._query(qout, processed=True)

    def _query(self, sql_query, vars=None, processed=False):
        """
        Execute SQL query `sql_query` using dictionary `vars` to interpolate it.
        If `processed=True`, `vars` is a `reparam`-style list to use
        instead of interpolating.

            >>> db = DB(None, {})
            >>> db.query("SELECT * FROM foo", _test=True)
            <sql: 'SELECT * FROM foo'>
            >>> db.query("SELECT * FROM foo WHERE x = $x", vars=dict(x='f'), _test=True)
            <sql: "SELECT * FROM foo WHERE x = 'f'">
            >>> db.query("SELECT * FROM foo WHERE x = " + sqlquote('f'), _test=True)
            <sql: "SELECT * FROM foo WHERE x = 'f'">
        """
        if not processed and not isinstance(sql_query, SQLQuery):
            sql_query = _reparam(sql_query, vars)

        db_cursor = self._db_cursor()
        self._db_execute(db_cursor, sql_query)

        if db_cursor.description:
            names = [x[0] for x in db_cursor.description]
            def iterwrapper():
                row = db_cursor.fetchone()
                while row:
                    yield Storage(dict(zip(names, row)))
                    row = db_cursor.fetchone()
            out = list(IterBetter(iterwrapper()))
            # out.__class__.__len__ = lambda self: int(db_cursor.rowcount)
            # out.list = lambda: [Storage(dict(zip(names, x))) for x in db_cursor.fetchall()]
        else:
            out = db_cursor.rowcount

        if not self.ctx.transactions:
            self.ctx.commit()
        return out

    def sql_clauses(self, fields, tables, where, group, having, order, limit, offset):
        return (
            ("SELECT", fields),
            ("FROM", sqllist(tables)),
            ("WHERE", where),
            ("GROUP BY", group),
            ("HAVING", having),
            ("ORDER BY", order),
            ("LIMIT", limit),
            ("OFFSET", offset)
        )

    def gen_clause(self, sql, val, vars):

        if isinstance(val, int):
            if sql == "WHERE":nout = 'id = ' + _sqlquote(val)
            else:
                nout = SQLQuery(val)
        elif isinstance(val, (list, tuple)):
            nout = []
            for item in val:
                if isinstance(item, dict):
                    nout.append(self._where_dict(item))
                elif isinstance(item, SQLQuery):
                    nout.append(item)
                else:
                    raise Exception("Unknow SQL:  %s" % str(item))
            nout = SQLQuery.join(nout, ' AND ')
        elif sql == "WHERE" and isinstance(val, dict):
            nout = self._where_dict(val)
        elif isinstance(val, SQLQuery):
            nout = val
        else:
            nout = _reparam(val, vars)

        def xjoin(a, b):
            if a and b: return a + ' ' + b
            # else: return a or b
            else: return ''
        return xjoin(sql, nout)

    def exec_sql(self, sql, args=None):
        db_cursor = self._db_cursor()
        sql_query = SQLQuery(sql)
        self._db_execute(db_cursor, sql_query)
        if db_cursor.description:
            names = [x[0] for x in db_cursor.description]
            def iterwrapper():
                row = db_cursor.fetchone()
                while row:
                    yield Storage(dict(zip(names, row)))
                    row = db_cursor.fetchone()
            out = list(IterBetter(iterwrapper()))
            # out.__len__ = lambda: int(db_cursor.rowcount)
            # out.list = lambda: [Storage(dict(zip(names, x))) for x in db_cursor.fetchall()]
        else:
            out = db_cursor.rowcount
        if not self.ctx.transactions:
            self.ctx.commit()
        return out

    exec_update = exec_sql

    def exec_insert(self, sql, args=None):
        db_cursor = self._db_cursor()
        sql_query = SQLQuery(sql)
        q1, q2 = self._process_insert_query(sql_query, '', '')
        self._db_execute(db_cursor, q1)
        self._db_execute(db_cursor, q2)
        try:
            out = db_cursor.fetchone()[0]
        except Exception as e:
            out = None

        if not self.ctx.transactions:
            self.ctx.commit()
        return out

    def transaction(self):
        return Transaction(self.ctx)


pool = {}


def singleton(cls, *args, **kwargs):
    def _wrap(*args2, **kwargs2):
        global pool
        host = kwargs2.get('host', '')
        port = kwargs2.get('port', '')
        db = kwargs2.get('db', '')
        key = '%s:%s:%s' % (host, port, db)
        if key not in pool:
            pool[key] = cls(*args2, **kwargs2)
        return pool[key]
    return _wrap


@singleton
class SqlPool(DB):
    def __init__(self, **config):

        db = import_driver(["MySQLdb", "pymysql"])

        if 'port' in config:
            config['port'] = int(config['port'])
        if 'charset' not in config:
            config['charset'] = 'utf8'
        elif config['charset'] is None:
            del config['charset']

        DB.__init__(self, db, config)
        self.supports_multiple_insert = True

    def _process_insert_query(self, query, tablename, seqname):
        return query, SQLQuery('SELECT last_insert_id();')

    def _get_insert_default_values_query(self, table):
        return "INSERT INTO %s () VALUES()" % table


def import_driver(drivers, preferred=None):
    """Import the first available driver or perferred driver
    """
    if preferred:
        drivers = [preferred]

    for d in drivers:
        try:
            return __import__(d, None, None, ['x'])
        except ImportError:
            pass
    raise ImportError("Unable to import " + " or ".join(drivers))


if __name__ == '__main__':
    config = {'host': '127.0.0.1', 'db': 'bar', 'user': 'root', 'charset': 'utf8', 'port': '3306',
              'passwd': '123123'}
    db = SqlPool(**config)
    # db.multiple_insert(table_name='test', obj_list=[{'name': "abc", 'age': 12}, {'name': "abc", 'age': 12}])
    print(db.insert(table_name='t1', obj_dict={'name': "abc", 'age': 12}))
    # # print(db.insert(table_name='test', obj_dict={'name': "abc"}))
    # # print(db.insert(table_name='test', obj_dict={'name': "abc"}))
    # # print(db.insert(table_name='test', obj_dict={'name': "abc"}))
    # #
    #
    # # db.update(table_name='test', where={'id__gte': 70}, update_dict={'age': 10})
    # ret = db.query(table_name='test', where={"id__in": [109, 111]}, fields=['*'], group_by='', having=None, page=1, page_num=10,
    #                order_by="id desc")
    # print(list(ret))

    # print(sqlor({'id__in': [1, 2, 3], 'id__gte': 653}))
    # out = db.query(table_name='test')
    # print(out)
    # print(db.delete(table_name='test', where={'id__gte': 0}))
    # db.query(table_name='test')
    # out = db.query(table_name='test', where=[{'id__gte': 0}, sqlor([{'age__in': [1, 2, 3]}, {'age__gte': 653}]),
    #                                          sqlor([{'id__in': [1, 2, 3]}, {'id__gte': 653}])])
    # print(out)

    # # print([i for i in db.exec_sql("select * from test")])
    # # print(db.exec_sql("delete from test where id=77"))
    # # print(db.exec_insert("insert into test(name) values('abcd')"))
    #
    # with db.transaction():
    # t = db.transaction()
    # obj_list = [{'name': 'abc_%d' % i, 'age': i} for i in range(10)]
    # out = db.multiple_insert(table_name='test', obj_list=obj_list)
    # while True:
    #     input()
    #     break
    # out = db.query(table_name='test')
    # for i in out:
    #     print(i)
    #
    # t.commit()

    # where = {'id': 27}
    # print(str(Field('1')))
    # update_dict = {"age": Field('age+1'), 'name': 'aaa'}
    # rowcount = db.update(table_name='test', where={"id": 109}, update_dict=update_dict)
    # print(rowcount)
    # sql = "UPDATE test SET age = %s WHERE id = %s"
    # params = ['`age+1`', 109]
    # db.test_execute(sql, params)
    #
    # import MySQLdb
    # config['port'] = int(config['port'])
    # config.pop('charset')
    # conn = MySQLdb.connect(**config)
    # cursor = conn.cursor()
    # cursor.execute(sql, params)

    #
    # rowcount = db.update(table_name='test', where={}, update_dict=update_dict)
    # print(rowcount)

    # where = {'id': 10}
    # rowcount = db.delete(table_name='test', where='')
    # print(rowcount)

    # ===
    # from utility.utils import SqlPool
    # db = SqlPool(**config)
    # print(db.exec_sql("select * from test"))
    # print(db.exec_sql("delete from test where id=77"))
    # print(db.exec_insert("insert into test(name) values('abcd')"))
    # print(Field(1))

    # obj_list = [{'name': "abc_%d" % i, 'age': i} for i in range(1, 11)]
    # duplicate = {"age": Field("VALUES(age)")}
    # for i in db.multiple_insert(table_name='test', obj_list=obj_list, duplicate=duplicate):
    #     print(i)


    # print(sqlor({"a": 1, "b": 2}))