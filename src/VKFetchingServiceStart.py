from FetchingServices.VKFetchingService.VKFetchersManager import VKFetchersManager
import config
import asyncio

def main():
    vk_fetchers_manager = VKFetchersManager(config.target_groups_ids)
    loop = asyncio.new_event_loop()
    vk_fetchers_start_task = loop.create_task(vk_fetchers_manager.start())
    loop.run_forever()
    print("end")

main()