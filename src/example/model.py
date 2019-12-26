from core.model import BaseTopicSchema, topic_setting
from core.service import SERVICE_REGISTRY

class City(BaseTopicSchema):
    name: str


@topic_setting(name="lal-user-topic", partitions=15, ttl=3570)
class User(BaseTopicSchema):
    name: str
    age: int
    city: City
