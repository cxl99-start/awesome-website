import www.orm as orm
import asyncio
from www.models import User,Blog,Comment

async def test(loop):
    await orm.create_pool(loop=loop,user='root',password='root',db='awesome')
    U=User(name='Test',email='test@qq.com',)