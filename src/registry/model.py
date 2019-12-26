from core.model import BaseTopicSchema, topic_setting

TTL = 3_153_600_000_000


@topic_setting("service-topic-registry", partitions=6, replicas=1, ttl=TTL)
class ServiceRegistry(BaseTopicSchema):
    name: str
    json_schema: dict
