def connect(dbindex=0,reuse=True):
    '''
    Initializes a Redis connection pool
    @param reuse: If True, make the redis connection shareable between thread. If False, create new connection
    '''
    if reuse is True and dbindex in redis_instances:
        return redis_instances[dbindex]
    # http://zerodivisible.com/python-redis-typeerror-an-integer-is-required/
    # connection pool is managed by this object
    # http://delog.wordpress.com/2013/08/16/handling-tcp-keepalive/,
    pool = redis.ConnectionPool(max_connections=30, host=settings['REDIS_HOST'], port=redis_port, db=dbindex,
                                password=settings['REDIS_PASSWORD'])
    if reuse is False:
        return redis.Redis(connection_pool=pool)
    redis_instances[dbindex] = redis.Redis(connection_pool=pool)
    return redis_instances[dbindex]


# ------------------------------------------------------
# Redis's queue
# ------------------------------------------------------
def enqueue(queue_name, message_, dbname=0):
    connect(dbname).rpush(queue_name, message_)


def dequeue(queue_name, dbname=0):
    item = connect(dbname).blpop(queue_name, timeout=1)
    if item:
        return item[1]
    return None


def queue_length(queue_name, dbname=0):
    """
    Total message in queue
    """
    return connect(dbname).llen(queue_name)


# ------------------------------------------------------
# Redis's key-value
# ------------------------------------------------------
def vset(key, value, dbname=0):
    """
    Sets a value to a key
    """
    return connect(dbname).set(key, value)


def vget(key, dbname=0):
    """
    Gets a value associated with a key
    """
    return connect(dbname).get(key)


def vdel(key, dbname=0):
    """
    Deletes a key and its value
    :param key: a string or a list
    """
    return connect(dbname).delete(key)


def vget_all(keys, dbname=0):
    return connect(dbname).mget(keys)


def vget_all_as_dict(keys, dbname=0):
    '''
    Returns a mapping between key and value

    Not found key will be assigned to None value
    :rtype keys: list
    @param keys: A list of keys
    @return: {key1: value1, key2: None} if key2 can not be found
    '''
    ret = connect(dbname).mget(keys)
    if ret:
        # Map two lists into a dictionary
        return dict(izip(keys, ret))
    return {}


def vsetnx(name, value, dbname=0):
    '''
    @see http://redis.io/commands/setnx
    '''
    return connect(dbname).setnx(name, value)


def vincrby(key, increment, dbname=0):
    """
    Increase a key's value by an amount
    @see: http://redis.io/commands/incrby

    :param key:
    :param increment:
    :param dbname:
    :return:
    """
    return connect(dbname).incrby(key, increment)


def vdecrby(key, increment, dbname=0):
    """
    Decrement a key's value by an amount
    @see: http://redis.io/commands/incrby

    :param key:
    :param increment:
    :param dbname:
    :return:
    """
    return connect(dbname).decr(key, amount=increment)


# ------------------------------------------------------
# Redis's set
# ------------------------------------------------------
def set_add(key, value, dbname=0):
    try:
        return connect(dbname).sadd(key, value)
    except:
        # list_add('list_retry_redis', ujson.dumps({'action':'set_add', 'key':key, 'value':value}), dbname)
        return None
