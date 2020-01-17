import asyncio,logging,aiomysql

def log(sql, args=()):
    logging.info('SQL: %s' % sql)

#创建一个连接池，每个http请求直接从连接池获取数据库连接，避免每次手动的去开启数据库和关闭
async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool  #定义全局变量
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),  #自动提交事务
        maxsize=kw.get('maxsize', 10),    #池中最多有是个连接对象
        minsize=kw.get('minsize', 1),
        loop=loop
    )

#执行select语句
async def select(sql, args, size=None):    #size可以决定取几条
    log(sql, args)
    with (await __pool) as conn:
        cur = await conn.cursor(aiomysql.DictCursor)
        await cur.execute(sql.replace('?', '%s'), args or ())   #用参数替换可以防止sql注入
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
        await cur.close()
        logging.info('rows returned: %s' % len(rs))
        return rs

#执行INSERT、UPDATE、DELETE语句，定义一个通用的execute()函数，这3种SQL的执行都需要相同的参数，以及返回一个整数表示影响的行数
async def execute(sql, args):
    log(sql)
    with (await __pool) as conn:
        try:
            cur = await conn.cursor()
            await cur.execute(sql.replace('?', '%s'), args)
            affected = cur.rowcount
            await cur.close()
        except BaseException as e:
            raise
        return affected

#元类，model只是一个基类，如果要将子类的映射信息取出来需要通过metaclass=ModelMetaclass
#任何继承与model的类都会自动通过ModelMetaclass扫描映射关系，存储到自身的类属性
class ModelMetaclass(type):
    # 元类必须实现__new__方法，当一个类指定通过某元类来创建，那么就会调用这个元类的__new__方法
    # __new__方法接受四个参数
    # cls是当前准备创建的类的对象
    # name为类的名字,如果创建user类,name就是user
    # bases类继承的父类的集合，如果创建user类，那么base就是Model
    # attrs为类的属性/方法集合，创建user类，则attrs就是一个包含user类属性的dict
    def __new__(cls, name, bases, attrs):
        # 排除Model基类本身，这些子类都通过Model继承元类
        if name=='Model':
            return type.__new__(cls, name, bases, attrs)
        # 获取table的名称，默认与类的名字相同
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        # 获取所有的字段名和字段值
        mappings = dict()
        # 仅用来存储费主键以外的其他字段，而且只存key
        fields = []
        # 仅保存主键的key
        primaryKey = None
        # 注意这里attrs的key是字段名，value是字段实例，不是字段的具体值
        # 比如User类的id=StringField(...) 这个value就是这个StringField的一个实例，而不是实例化
        # 的时候传进去的具体id值
        for k, v in attrs.items():
            # isinstance 方法用于判断v是否是一个Field
            if isinstance(v, Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    if primaryKey:    # 找到主键:
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)
        # 保证必须有一个主键
        if not primaryKey:
            raise RuntimeError('Primary key not found.')
        # 这里的目的是去除类属性，为什么要去除呢，因为已经记录袋了mappings，fields等变量里面了
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings # 保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey # 主键属性名
        attrs['__fields__'] = fields # 除主键外的属性名
        # 构造默认的SELECT, INSERT, UPDATE和DELETE语句:
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)


#定义ORM映射的基类Model
class Model(dict, metaclass=ModelMetaclass):  #python动态语言，函数和类不是编译时定义的，而是动态创建的，

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):    #在类中没有这个属性时，才回调用这个方法，来提示用户，属性不存在
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):    #试图给对象赋值的时候会被调用
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    #取默认值，字段类中有一个默认属性，默认值也可以是函数
    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default    #三元运算，如果函数可调用，就返回iled.default()，否则，反之
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

    @classmethod   #不需要实例化就可以调用类
    async def findAll(cls, where=None, args=None, **kw):
        ## find objects by where clause
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        ## find number by select and where
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

    @classmethod
    async def find(cls, pk):
        ## find object by primary key
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warn('failed to insert record: affected rows: %s' % rows)

    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)

    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warn('failed to remove by primary key: affected rows: %s' % rows)


#字段类的实现
class Field(object):

    def __init__(self, name, column_type, primary_key, default):
        self.name = name  # 字段名
        self.column_type = column_type  # 字段数据类型
        self.primary_key = primary_key  # 是否为主键
        self.default = default  # 有无默认值

    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)

class StringField(Field):

    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)

class BooleanField(Field):

    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)

class IntegerField(Field):

    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)

class FloatField(Field):

    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)

class TextField(Field):

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)


def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)





