import asyncio
import argparse
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
        result = await asyncio.gather(
            *[
                producer.send(
                    key=f"Onna-{age}",
                    value=User(
                        name=f"Onna-{age}", age=age + 1, city=City(name="Durham")
                    )
                )
                for age in range(count)
            ]
        )
        for item in result:
            print(item.topic_partition, item.offset)
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=1)
    arguments, _ = parser.parse_known_args()
    asyncio.run(send(arguments.count))
