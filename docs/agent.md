
```python
import asyncio

from core.decorator import agent
from core.model import BaseTopicSchema, topic_setting


class City(BaseTopicSchema):
    name: str


@topic_setting(name="user-test-topic", partitions=1, replicas=2, ttl=3600000)
class User(BaseTopicSchema):
    name: str
    age: int
    city: City


@agent(User)
async def agent_worker(stream):
    async for key, user in stream.items():
        print(user.name)


async def send(agent, count=100):
    await asyncio.gather(
        *[
            agent_worker.send(
                key="Onna",
                value=User(name=f"Onna-{age}", age=age + 1, city=City(name="Durham")),
            )
            for age in range(count)
        ]
    )


async def consume(agent):
    try:
        await agent_worker.start()
    finally:
        await agent_worker.stop()


if __name__ == "__main__":
    agent_worker.configure(
        bootstrap_servers="kafka-intra01.intra.onna.internal:9092",
        producer_config={"key_serializer": lambda key: key.encode(),},
        concurrency=2,
        service=True,
    )
    # asyncio.run(send(agent_worker))
    # asyncio.run(main(agent_worker))
```
