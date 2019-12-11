from core.model import BaseTopicSchema, topic_setting


class City(BaseTopicSchema):
    name: str


@topic_setting(name="t-user-topic", partitions=5, replicas=2, ttl=9090909)
class User(BaseTopicSchema):
    name: str
    age: int
    city: City
