from typing import Dict
from core.decorator import consumer
from core.model import BaseTopicSchema
from registry.model import ServiceRegistry


SERVICE_REGISTRY: Dict[str, BaseTopicSchema] = dict()


@consumer(ServiceRegistry)
async def worker(stream):
    async for name, schema in stream.items():
        SERVICE_REGISTRY.setdefault(name, schema)


async def start(**settings):
    worker.configure(**settings)
    await worker.start()


def get(name, default=None) -> BaseTopicSchema:
    return REGISTRY.get(name, default=default)


async def add(model: BaseTopicSchema = None) -> BaseTopicSchema:
    await worker.send(
        key=model.topic_name(),
        value=ServiceRegistry(name=name, schema=model.schema())
    )

