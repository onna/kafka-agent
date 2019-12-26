import asyncio

from core.decorator import consumer
from example.model import City, User

def do_something(stream, msg):
    print(str(stream._client).strip(">").split()[1], msg.topic, msg.partition, msg.offset, msg.value.age, msg.timestamp)

@consumer(User)
async def worker(stream):
    async for msg in stream:
        do_something(stream, msg)

if __name__ == "__main__":
    worker.configure(
        bootstrap_servers="kafka-intra01.intra.onna.internal:9092",
        concurrency=2
    )
    asyncio.run(worker.start())
