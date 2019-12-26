import asyncio
import uuid

from core.component import ConsumerAgentWorker, ConsumerComponent, ProducerComponent
from core.model import BaseTopicSchema


class consumer:
    def __init__(self, model: BaseTopicSchema, agent_id: str = None, group_id=None):
        self.model = model
        self.client_id = agent_id or f"cid-{self.model.topic_name()}"
        self.group_id = group_id or f"cg-{self.model.topic_name()}"
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
                                "client_id": f"{self.client_id}-{str(uuid.uuid1())}",
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
        try:
            await asyncio.gather(*[consumer.run() for consumer in self.consumers])
        finally:
            await self.stop()

    async def start(self):
        self.init_consumers()
        await self._start()

    async def stop(self):
        for consumer in self.consumers:
            await consumer.stop()


class agent(consumer):
    """
        @agent(User)
        async def agent_worker(stream: ConsumerComponent):
            async for msg in stream:
                print(msg)

        user = User(name="test", ...)
        agent_worker.configure(...)
        await agent_worker.send(key=user.name, value=user)

        try:
            await agent_worker.start()
        finally:
            await agent_worker.stop()
    """

    def __init__(self, model: BaseTopicSchema, agent_id: str = None):
        super().__init__(model, agent_id=agent_id)
        self.client_id = agent_id or f"aggent-cid-{self.model.topic_name()}"
        self.group_id = f"agent-cgid-{self.model.topic_name()}"

    def init_producer(self):
        if getattr(self, "producer", None) is None:
            self.producer = ProducerComponent(
                self.model,
                **{
                    **self.producer_config,
                    **{"client_id": f"producer-{self.client_id}"},
                },
            )

    # def init_service(self):
    #     if not self.is_service:
    #         return
    #     if not hasattr(self, "service_consumer"):
    #         self.service_consumer = ConsumerComponent(
    #             self.model,
    #             **{
    #                 **self.consumer_config,
    #                 **{
    #                     "client_id": f"svc-consumer-{self.client_id}"
    #                 },
    #             },
    #         )
    #         consumer_registry[self.model.topic] = self

    def configure(self, producer_config=None, consumer_config=None, **config):
        self.concurrency = config.pop("concurrency", 1)
        # self.is_service = config.pop("service", False)
        self.producer_config = producer_config or {}
        self.consumer_config = consumer_config or {}
        self.producer_config = {**self.producer_config, **config}
        self.consumer_config = {**self.consumer_config, **config}
        self.config = config

    async def start(self):
        self.init_consumers()
        # self.init_service()
        consumer_tasks = [consumer.run() for consumer in self.consumers]
        # if self.is_service:
        #     consumer_tasks.append(self.service_consumer.start())
        await asyncio.gather(*consumer_tasks)

    async def stop(self):
        await super().stop()
        await self.producer.stop()

    async def send(self, value: BaseTopicSchema = None, **kwargs):
        if not hasattr(self, "producer"):
            self.init_producer()
            await self.producer.start()
        return await self.producer.send(value=value, **kwargs)
