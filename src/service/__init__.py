import json
import asyncio
from starlette.applications import Starlette
from service.producer import Producer
from core.task_vars import settings

from starlette.responses import JSONResponse
from starlette.routing import Route, Mount, WebSocketRoute

import registry



class Application(Starlette):

    async def initialize(self, kafka_brokers=None):
        self.producer = Producer(
            loop=asyncio.get_running_loop(),
            bootstrap_servers=kafka_brokers
        )
        await self.producer.start()
        self.registry_worker = asyncio.create_task(
            registry.start(
                loop=asyncio.get_running_loop(),
                bootstrap_servers=kafka_brokers,
                producer_config={
                    "key_serializer": lambda key: key.encode()
                }
            )
        )

    async def finalize(self):
        await self.producer.stop()
        await self.registry_worker.cancel()


app = Application(debug=True)


@app.route("/topic/", methods=["GET"])
def get_topics(request):
    return JSONResponse(list(registry.SERVICE_REGISTRY.keys()))


@app.route("/topic/{topic}", methods=["GET"])
def get_topic_schema(request):
    topic = request.path_params['topic']
    if topic not in registry.SERVICE_REGISTRY:
        return JSONResponse({"unregistered_topic": topic}, status_code=412)
    return JSONResponse(registry.SERVICE_REGISTRY[topic].schema())


@app.route("/topic/{topic}", methods=["POST"])
async def send(request):
    data = await request.json()
    topic = request.path_params['topic']
    if topic not in registry.SERVICE_REGISTRY:
        return JSONResponse({"unregistered_topic": topic}, status_code=412)

    try:
        result = await request.app.producer.send_json(
            topic,
            value=data,
            schema=registry.SERVICE_REGISTRY[topic].schema()
        )
    except Exception:
        return JSONResponse({"error": "jsonschema validation"}, status_code=412)

    return JSONResponse({"Record": result._asdict()})
