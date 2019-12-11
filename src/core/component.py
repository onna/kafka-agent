import asyncio
import operator
from dataclasses import dataclass
from typing import Coroutine

import aiokafka
from core.model import BaseTopicSchema
from kafka.admin import NewTopic
from kafka.admin.client import KafkaAdminClient
from kafka.errors import TopicAlreadyExistsError


class ProducerComponent(aiokafka.AIOKafkaProducer):
    def __init__(self, model: BaseTopicSchema, **config):
        self.model = model
        self.bootstrap_servers = config.get("bootstrap_servers", "localhost:9092")
        config.setdefault("loop", asyncio.get_event_loop())
        super().__init__(**config)
        if hasattr(self.model, "_topic_config"):
            self.create_topic(self.model.topic_name(), **self.model._topic_config)

    @property
    def admin_client(self):
        if not hasattr(self, "_admin_client"):
            self._admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers
            )
        return self._admin_client

    def _create_topic(self, topic, partitions=1, replicas=2, ttl=None):
        topic_configs = {}
        if ttl is not None:
            topic_configs["retention.ms"] = ttl
        topic = NewTopic(topic, partitions, replicas, topic_configs=topic_configs)
        return self.admin_client.create_topics([topic])

    def create_topic(self, *args, **kwargs):
        try:
            return self._create_topic(*args, **kwargs)
        except TopicAlreadyExistsError:
            return

    async def send(self, value: BaseTopicSchema = None, **kwargs):
        assert isinstance(value, self.model), "Invalid data type"
        sent = await super().send(
            self.model.topic_name(), value=value.json().encode(), **kwargs
        )
        return await sent


class ConsumerComponent(aiokafka.AIOKafkaConsumer):
    def __init__(self, model: BaseTopicSchema, **config):
        self.model = model
        config.setdefault("loop", asyncio.get_event_loop())
        rebalance_listener = config.pop(
            "rebalance_listener", aiokafka.ConsumerRebalanceListener
        )
        super().__init__(
            **{
                **config,
                **{
                    "key_deserializer": lambda key: key.decode("utf-8")
                    if key
                    else None,
                    "value_deserializer": lambda value: self.model(
                        **self.model.deserializer(value)
                    ),
                },
            }
        )
        self.subscribe(topics=[self.model.topic_name()], listener=rebalance_listener())

    async def filter(self, on="key", op="eq", value=None):
        try:
            op = {
                "lt": operator.lt,
                "le": operator.le,
                "eq": operator.eq,
                "ne": operator.ne,
                "ge": operator.ge,
                "gt": operator.gt,
            }[op]
        except KeyError:
            raise Exception("Invalid operation type.")

        async for msg in self:
            if op(value, getattr(msg, on)):
                yield msg

    async def items(self, meta=False):
        async for msg in self:
            if meta is True:
                metadata = dict(
                    topic=msg.topic,
                    partition=msg.partition,
                    offset=msg.offset,
                    timestamp=msg.timestamp,
                    timestamp_type=msg.timestamp_type,
                    checksum=msg.checksum,
                    serialized_key_size=msg.serialized_key_size,
                    serialized_value_size=msg.serialized_value_size,
                    headers=msg.headers,
                )
                yield msg.key, msg.value, metadata
            else:
                yield msg.key, msg.value

    async def take(self, count, every=5 * 1000):
        ...

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.stop()


@dataclass
class ConsumerAgentWorker:
    coro: Coroutine
    consumer: ConsumerComponent = None

    async def run(self):
        async with self.consumer as stream:
            await self.coro(stream)

    async def stop(self):
        await self.consumer.stop()
