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
        if not hasattr(self._admin_client):
            self._admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers
            )
        return self._admin_client

    def _create_topic(self, topic, partitions=1, replicas=2, retention_ms=None):
        topic_configs = {}
        if retention_ms is not None:
            topic_configs["retention.ms"] = retention_ms
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

    async def items(self):
        async for msg in self:
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


class consumer:
    def __init__(self, model: BaseTopicSchema, agent_id: str = None):
        self.model = model
        self.client_id = agent_id or f"consumer-cid-{self.model.topic_name()}"
        self.group_id = f"consumer-cg-{self.model.topic_name()}"
        self.concurrency = 1
        self.consumers = list()
        self.tasks = list()
        self.worker = None

    def __call__(self, worker):
        self.worker = worker
        return self

    def init_consumers(self):
        if not self.consumers:
            self.consumers = [
                ConsumerAgentWorker(
                    coro=self.worker,
                    consumer=ConsumerComponent(
                        self.model,
                        **{
                            **self.consumer_config,
                            **{
                                "client_id": f"consumer-{self.client_id}",
                                "group_id": self.group_id,
                            },
                        },
                    ),
                )
                for _ in range(self.concurrency)
            ]

    def configure(self, **config):
        self.concurrency = config.pop("concurrency", 1)
        self.consumer_config = config or {}

    async def _start(self):
        await asyncio.gather(*[consumer.run() for consumer in self.consumers])

    async def start(self):
        self.init_consumers()
        await self._start()

    async def stop(self):
        for consumer in self.consumers:
            await consumer.stop()


class agent(consumer):
    """[summary]
    
        @agent(User)
        async def agent_worker(stream: ConsumerComponent):
            async for msg in stream:
                print(msg)

        user = User(name="test", ...)
        agent_worker.configure(...)
        await agent_worker.send(key=user.name, value=user)
        await agent_worker.consume(from_=None)
    """

    def __init__(self, model: BaseTopicSchema, agent_id: str = None):
        super().__init__(model, agent_id=agent_id)
        self.client_id = agent_id or f"aggent-cid-{self.model.topic_name()}"
        self.group_id = f"agent-cg-{self.model.topic_name()}"

    def init_producer(self):
        if getattr(self, "producer", None) is None:
            self.producer = ProducerComponent(
                self.model,
                **{
                    **self.producer_config,
                    **{"client_id": f"producer-{self.client_id}"},
                },
            )

    def configure(self, producer_config=None, consumer_config=None, **config):
        self.concurrency = config.pop("concurrency", 1)
        self.is_service = config.pop("service", False)
        self.producer_config = producer_config or {}
        self.consumer_config = consumer_config or {}
        self.producer_config = {**producer_config, **config}
        self.consumer_config = {**consumer_config, **config}
        self.config = config

    async def stop(self):
        await super().stop()
        await self.producer.stop()

    async def send(self, value: BaseTopicSchema = None, **kwargs):
        if not hasattr(self, "producer"):
            self.init_producer()
            await self.producer.start()
        return await self.producer.send(value=value, **kwargs)


if __name__ == "__main__":
    from core.model import topic_name

    class City(BaseTopicSchema):
        name: str

    @topic_name("user-topic")
    class User(BaseTopicSchema):
        name: str
        age: int
        city: City

    @agent(User)
    async def agent_worker(consumer):
        async for msg in consumer:
            print(msg)

    async def consumer_worker():
        consumer = ConsumerComponent(User)
        async with consumer as stream:
            async for msg in stream:
                print(msg)
