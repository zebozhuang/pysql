# coding: utf-8

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


if __name__ == '__main__':
    import doctest
    doctest.testmod()