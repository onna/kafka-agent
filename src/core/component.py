import asyncio
import operator
import aiokafka
import jsonschema
from typing import Dict
from typing import List
from typing import Union
from typing import Coroutine
from core.model import BaseTopicSchema

from kafka.admin import NewTopic
from kafka.admin import NewPartitions
from kafka.admin import ConfigResource
from kafka.admin.client import KafkaAdminClient
from kafka.errors import TopicAlreadyExistsError

from dataclasses import dataclass


class TopicManager(KafkaAdminClient):

    def __init__(self, model: BaseTopicSchema, **config):
        self.model = model
        super().__init__(**config)
        self.sync_topic()
        # refresh KafkaAdminClient connection
        self.close()
        super().__init__(**config)

    def sync_topic(self):
        result = None
        if hasattr(self.model, "_topic_config"):
            if self.topic_config is None:
                result = self.create_topic()
            if self.topic_config["partitions"] != self.model._topic_config["partitions"]:
                count = self.model._topic_config["partitions"]
                result = self.update_partition_count(count)
            if self.topic_config["ttl"] != self.model._topic_config["ttl"]:
                ttl = self.model._topic_config["ttl"]
                result = self.alter_configs(
                    [ConfigResource('topic', self.topic_name, configs={"retention.ms": ttl})]
                )
        return result

    @property
    def topic_name(self):
        return self.model.topic_name()

    @property
    def broker_config(self):
        broker_id = self._client.cluster.controller.nodeId
        return self.describe_configs([ConfigResource('broker', broker_id)])

    def _get_config(self, name):
        return int(
            list(
                filter(lambda c: c[0] == name, self.broker_config.resources[-1][-1])
            )[0][1]
        )

    @property
    def default_partition_count(self):
        return self._get_config("num.partitions")

    @property
    def default_replica_count(self):
        return self._get_config("min.insync.replicas")

    def _create_topic(self, topic, partitions=1, ttl=None):
        topic_configs = {}
        if ttl is not None:
            topic_configs["retention.ms"] = ttl
        topic = NewTopic(topic, partitions, self.default_replica_count, topic_configs=topic_configs)
        return self.create_topics([topic])


    def create_topic(self):
        try:
            return self._create_topic(self.model.topic_name(), **self.model._topic_config)
        except TopicAlreadyExistsError:
            return

    def update_partition_count(self, count):
        return self.create_partitions({self.topic_name: NewPartitions(total_count=count)})

    @property
    def topic_config(self):
        conf = self.describe_configs([ConfigResource('topic', self.model.topic_name())])
        if conf.resources[-1][-1]:
            return {
                "partitions": self.topic_partition_count,
                "ttl": int(list(filter(lambda c: c[0] == "retention.ms", conf.resources[-1][-1]))[0][1])
            }
        return

    @property
    def topic_partition_count(self):
        partitions = self._client.cluster.available_partitions_for_topic(self.topic_name)
        if partitions:
            return len(partitions)
        return

    def delete_topic(self, *args, **kwargs):
        ...



class ProducerComponent(aiokafka.AIOKafkaProducer):
    def __init__(self, model: BaseTopicSchema, **config):
        self.model = model
        self.bootstrap_servers = config.get("bootstrap_servers", "localhost:9092")
        config.setdefault("loop", asyncio.get_event_loop())
        self.tm = TopicManager(self.model, bootstrap_servers=self.bootstrap_servers)
        super().__init__(**config)

    async def send(self, value: Union[BaseTopicSchema, Dict] = None, **kwargs):

        if isinstance(value, dict):
            jsonschema.validate(value, self.model.schema())
            value=json.loads(value).encode()
        elif isinstance(value, self.model):
            value=value.json().encode()
        else:
            raise Exception("Invalid data")

        sent = await super().send(
            self.model.topic_name(), value=value, **kwargs
        )
        return await sent

class ConsumerComponent(aiokafka.AIOKafkaConsumer):
    def __init__(self, model: BaseTopicSchema, **config):
        self.model = model
        self.bootstrap_servers = config.get("bootstrap_servers", "localhost:9092")
        config.setdefault("loop", asyncio.get_event_loop())
        self.tm = TopicManager(self.model, bootstrap_servers=self.bootstrap_servers)
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

    async def filter(self, *, on=None, op=None, value=None, key=lambda msg: True):
        if key is None:
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
            key = lambda msg: op(value, getattr(msg, on))

        async for msg in self:
            if key(msg):
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

    async def map(self, func=lambda msg: msg):
        async for msg in self:
            yield func(msg)

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
        print(f"starting consumer: {self.consumer._client}")
        async with self.consumer as stream:
            await self.coro(stream)

    async def stop(self):
        print(f"Stopping consumer: {self.consumer._client}")
        await self.consumer.stop()
