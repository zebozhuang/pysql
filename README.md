PySQL
-----
PySQL is a MySQL client library written in Python, it can make 
**CURD** operations easy and convenient without writing raw SQL.

## Tutorial

### 1 Configure

#### 1.1 Configure in a toml file
```
    host = "127.0.0.1"
    port = 3306
    user = "root"
    passwd = "123123"
    charset = "utf8"
    db = "bar"
    mincached = 3
    maxcached = 3
   # maxshared = 0
```
Please install [Python **toml**](https://github.com/uiri/toml) to read this file.

#### 1.2 Configure in a dictionary form

```
    config = {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "root",
        "passwd": "123123",
        "charset": "utf8",
        "db": "bar",
        "mincached": 3,
        "maxcached": 3,
        "maxshared": 0
    }
```

### 2 Instance

```
    from pysql.pool import SQLPool

    pool = SQLPool(**config)
```

### 3 Insert

```
    obj = {'name': 'abc', 'age': 10}
    insert_id = pool.insert(table='t1', obj=obj)
```

```
    objs = [
        {'name': 'a', 'age': 1},
        {'name': 'b', 'age': 2},
        {'name': 'c', 'age': 3},
    ]
    insert_ids = pool.insertmany(table='t1', objs=objs)
```

### 4 Update
**obj** is the updating data and **where** is query condition.

```
    Case: id = 3
    obj = {'age': 10}
    where = {'id': 3}  or where = {'id__eq': 3}
    affected_rows = pool.update(table='t1', where=where, obj=obj)
```

```
    # Case: 12 <= id <= 20
    where = {'id__gte': 12, 'id__lte': 20}
    affected_rows = pool.update(table='t1', where=where, obj=obj)
```
```
    # Case: id != 4
    where = {'id__neq': 4}
    affected_rows = pool.update(table='t1', where=where, obj=obj)
```

### 5. Delete

```
    # Case: id in (1, 2, 3)
    affected_rows = pool.delete(table='t1', where={'id__in': [1, 2, 3]})
```

### 6. Select
```
    affected_rows = pool.query(table='t1', where={'id__in': [1, 2, 3]})
```