import asyncio
from VKPulling import groupPullingFactoryTask

class Controller:
    def __init__(self, initGroups: dict = dict()):
        self.groups: dict = initGroups
        self.loop = asyncio.get_running_loop()
    
    def clear(self):
        for task in self.groups.items:
            try:
                task.cancel()
            except asyncio.CancelledError as e:
                print(e)

    def add(self, group_id: str):
        if self.groups.get(group_id) != None:
            return

        print("added for pulling " + group_id)
        task = self.loop.create_task(groupPullingFactoryTask(group_id, sleepTime=5))
        self.groups[group_id] = task
        task.add_done_callback(self.groups.pop(group_id))

    def remove(self, group_id: str):
        if self.groups.get(group_id) != None:
            self.groups[group_id].cancel()
    

async def controllerTask():
    controller = Controller()
    controller.add("miptdndleague")
    controller.add("lovelycosplay")