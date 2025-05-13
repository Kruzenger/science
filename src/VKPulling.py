from vkbottle import VKAPIError
from vkbottle.api import API
from secrets import VK_ACCESS_TOKEN
import asyncio

class VKPulling:
    def __init__(self, vk_token: str = VK_ACCESS_TOKEN):
        self.api = API(vk_token)

    async def pullGroupPosts(self, group_id):
        print("started pulling " + group_id)
        
        posts = []
        POST_COUNT = 100 # max number of posts available to collect
        posts = await self.api.request("wall.get", {"domain": group_id, "count": POST_COUNT})

        print("ended pulling " + group_id)
        return posts
        
vk_pulling = VKPulling()

async def printData(data):
    print(await data)

async def groupPullingFactoryTask(group_id, sleepTime):
    loop = asyncio.get_running_loop()
    while True:
        loop.create_task(vk_pulling.pullGroupPosts(group_id=group_id))
        await asyncio.sleep(sleepTime)