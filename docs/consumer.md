
```python
import asyncio

from core.decorator import consumer
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
