import asyncio
from VKPulling import groupPullingFactoryTask

class Controller:
    def __init__(self, initGroups: dict = dict()):
        self.groups: dict = initGroups
        self.loop = asyncio.get_running_loop()
    
    async def clear(self):
        for task in self.groups.items:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            print("removed all targets for pulling")
            self.groups.clear()
                

    async def add(self, group_id: str):
        if self.groups.get(group_id) != None:
            return

        task = self.loop.create_task(groupPullingFactoryTask(group_id, sleepTime=5))
        self.groups[group_id] = task
        print(f"added for pulling: {group_id}")

    async def remove(self, group_id: str):
        if self.groups.get(group_id) != None:
            self.groups[group_id].cancel()
            try:
                await self.groups[group_id]
            except asyncio.CancelledError:
                print(f"removed for pulling: {group_id}")
                self.groups.pop(group_id)
    
async def controllerTask():
    controller = Controller()
    await controller.add("public192867633")