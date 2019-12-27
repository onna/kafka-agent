
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
