from vkbottle import VKAPIError
from vkbottle.api import API
from mySecrets import VK_ACCESS_TOKEN
import asyncio

class VKPulling:
    def __init__(self, vk_token: str = VK_ACCESS_TOKEN):
        self.api = API(vk_token)
        self.last_post = dict()

    async def pullGroupPosts(self, group_id, iteration_limit: int = 10):
        print(f"started pulling: {group_id}")

        posts = []
        last_post_id = self.last_post.get(group_id)

        if (last_post_id == None):
            try:
                print(f"pulling for the first time: " + group_id)
                posts_pack = await self.api.request("wall.get", {"domain": group_id, "count": 2})
                posts = posts_pack["response"]["items"]
            except VKAPIError as error:
                print(f"Couldn't get new post id for group_id = {group_id}. Error: {error}")
        else:
            POST_COUNT = 10 # max number of posts available to collect
            for i in range(iteration_limit):
                posts_pack = await self.api.request("wall.get", {"domain": group_id, "count": POST_COUNT, "offset": POST_COUNT*i})
                new_posts = [post for post in posts_pack["response"]["items"] if post["id"] is not None and post["id"] > last_post_id]
                if(len(new_posts) == 0):
                    break
                else:
                    posts.extend(new_posts)
            
        if(len(posts) != 0):
            self.last_post[group_id] =  max([post["id"] for post in posts])
        
        print(self.last_post[group_id])

        print(f"ended pulling: {group_id}")
        return posts
        
vk_pulling = VKPulling()

async def printData(data):
    print(await data)

async def groupPullingFactoryTask(group_id, sleepTime):
    loop = asyncio.get_running_loop()
    while True:
        loop.create_task(vk_pulling.pullGroupPosts(group_id=group_id))
        await asyncio.sleep(sleepTime)