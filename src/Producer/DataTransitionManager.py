from VKPullingManager import VKPullingManager
from KafkaManager import KafkaManager
from config import target_groups_ids

class DataTransitionManager:
    def __init__(self):
        self.vk_pulling_manager = VKPullingManager()
        self.kafka_manager = KafkaManager()

    async def pullDataAndSend(self, group_id:str):
        posts = await self.vk_pulling_manager.pullGroupPosts(group_id)
        for post in posts:
            self.kafka_manager.send(topic_name=group_id, data=post)
        return posts

DATA_TRN_MNG: DataTransitionManager = DataTransitionManager()