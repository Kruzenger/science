import VKPulling
import asyncio
import time

vk_pulling = VKPulling.VKPulling()

async def printData(data):
    print(await data)

async def grouptask(group_id):
    loop = asyncio.get_running_loop()
    while True:
        loop.create_task(printData(vk_pulling.pullGroupPosts(group_id=group_id)))
        await asyncio.sleep(1)

group_ids = ["miptdndleague", "lovelycosplay"]

def main(): 
    loop = asyncio.new_event_loop()

    for group_id in group_ids:
        task = loop.create_task(grouptask(group_id))
    
    loop.run_forever()
    return 0

main()
