# Agent

```python
import asyncio

from core.decorator import agent
from example.model import City, User


@agent(User)
async def agent_worker(stream):
    async for key, user, _ in stream.items(meta=True):
        print(user.name, _)


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


async def run(agent):
    await send(agent)
    await consume(agent)


if __name__ == "__main__":
    agent_worker.configure(
        bootstrap_servers="kafka-intra01.intra.onna.internal:9092",
        producer_config={"key_serializer": lambda key: key.encode(),},
        concurrency=2,
        service=True,
    )
    asyncio.run(run(agent_worker))
```

# Producer


```python
import asyncio
from core.component import ProducerComponent
from example.model import City, User


async def send(count=100):
    producer = ProducerComponent(
        User,
        key_serializer=lambda key: key.encode(),
        bootstrap_servers="kafka-intra01.intra.onna.internal:9092",
    )
    await producer.start()
    try:
        print(
            await asyncio.gather(
                *[
                    producer.send(
                        key=f"Onna-{age}",
                        value=User(
                            name=f"Onna-{age}", age=age + 1, city=City(name="Durham")
                        ),
                    )
                    for age in range(count)
                ]
            )
        )
    finally:
        await producer.stop()


async def sync_send(count=100):
    producer = ProducerComponent(
        User,
        key_serializer=lambda key: key.encode(),
        bootstrap_servers="kafka-intra01.intra.onna.internal:9092",
    )
    await producer.start()
    try:
        for age in range(count):
            print(
                await producer.send(
                    key=f"Onna-{age}",
                    value=User(
                        name=f"Onna-{age}", age=age + 1, city=City(name="Durham")
                    ),
                )
            )
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(send())

```

# Consumer


```python
import asyncio

from core.decorator import consumer
from example.model import City, User


@consumer(User)
async def worker(stream):
    async for user in stream:
        print(user)


if __name__ == "__main__":
    worker.configure(
        bootstrap_servers="kafka-intra01.intra.onna.internal:9092", concurrency=2
    )
    asyncio.run(worker.start())

```
