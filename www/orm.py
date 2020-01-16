import asyncio,logging,aiomysql

def log(sql,args=()):
    logging.info("SQL:%s"%sql)

#创建一个连接池，每个http请求直接从连接池获取数据库连接，避免每次手动的去开启数据库和关闭数据库
async def create_pool(loop,**kw):
    logging.info('create database connection pool..... ')
    global __pool  #定义全局变量
    __pool=await aiomysql.create_pool(
        host=kw.get('host','localhost'),
        port=kw.get('post',3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset','utf-8'),
        autocommit=kw.get('autocommit',True),
        maxsize=kw.get('maxsize',10),
        minsize=kw.get('minsize',1),
        loop=loop
    )

#执行select语句
async def select(sql,args,size=None):
    log(sql,args)
    global __pool
    with (await __pool) as conn:
        cur=await conn.cursor(aiomysql.DictCursor)
        await cur.execute(sql.replace('?','%s'),args or ())
        if size:
            rs=await cur.fetchmany(size)
        else:
            rs=await cur.fetchall()
        await cur.close()
        logging.info('rows returned :%s'%len(rs))
        return rs
