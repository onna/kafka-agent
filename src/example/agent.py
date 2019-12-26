import asyncio
from core.decorator import agent
from example.model import City, User


@agent(User)
async def agent_worker(stream):
    async for key, user, _ in stream.items(meta=True):
        print(id(stream), key, _)


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
        await agent.start()
    finally:
        await agent.stop()


async def run(agent):
    await send(agent)
    await consume(agent)


if __name__ == "__main__":
    agent_worker.configure(
        bootstrap_servers="kafka-intra01.intra.onna.internal:9092",
        producer_config={"key_serializer": lambda key: key.encode()},
        concurrency=2
    )
    asyncio.run(run(agent_worker))
