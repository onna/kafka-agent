from typing import Dict

from core.core import agent
from core.model import BaseTopicSchema
from registry import REGISTRY
from registry.model import Topic

REGISTRY: Dict[str, BaseTopicSchema] = dict()


@agent(Topic)
async def registry_agent(stream):
    async for name, topic in stream.items():
        REGISTRY.setdefault(name, topic)


def lookup(name, default=None) -> BaseTopicSchema:
    return REGISTRY.get(name, default=default)


async def register(name, model: BaseTopicSchema = None) -> BaseTopicSchema:
    await registry_agent.send(key=name, value=Topic(name=name, schema=model))
