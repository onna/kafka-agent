from core.model import BaseTopicSchema, topic_setting

TTL = 31_536_000_000_000


@topic_setting("service-topic-registry", partitions=6, ttl=TTL)
class ServiceRegistry(BaseTopicSchema):
    name: str
    json_schema: dict
