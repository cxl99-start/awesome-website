import www.orm as orma
import asyncio
from www.models import User,Blog,Comment

async def test(loop):
    await orma.create_pool(loop=loop,host='localhost', user='www-data', password='www-data', db='awesome')
    u = User(name='Test', email='test@qq.com', passwd='1234567890', image='about:blank')
    await u.save()
    #添加到数据库后需要关闭连接池，否则会报错 RuntimeError: Event loop is closed
    orma.__pool.close()
    await orma.__pool.wait_closed()
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test(loop))
    loop.close()