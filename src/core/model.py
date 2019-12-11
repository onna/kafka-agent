import json

from kafka.admin import NewTopic
from pydantic import BaseModel


def topic_name(name):
    def wrapped_f(cls):
        cls._topic = name
        return cls

    return wrapped_f


def topic_setting(name: str, **config):
    def wrapped_f(cls):
        cls._topic = name
        if config:
            cls._topic_config = {
                "partitions": config.get("partitions", 1),
                "replicas": config.get("replicas", 2),
                "ttl": config.get("ttl"),
            }
        return cls

    return wrapped_f


class BaseTopicSchema(BaseModel):
    @property
    def topic(self):
        return self.topic_name()

    @classmethod
    def topic_name(cls):
        if hasattr(cls, "_topic"):
            return cls._topic
        return cls.__name__

    @classmethod
    def deserializer(cls, value):
        return json.loads(value.decode("utf-8"))
