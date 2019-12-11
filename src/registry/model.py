from core.model import BaseTopicSchema, topic_name


@topic_name("agent-topic-registry")
class Topic(BaseTopicSchema):
    name: str
    schema: BaseTopicSchema
