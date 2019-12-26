from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route, Mount, WebSocketRoute
import registry


class Registry(dict):
    def __setitem__(self, key, value):
        if key in self:
            raise Exception("Duplicated key")
        super().__setitem__(key, value)

    def add(self, model):
        self.__setitem__(model.topic_name(), model)


SERVICE_REGISTRY = Registry()

# async def get_from_kafka(consumer, metadata, default=None):
#     tp = TopicPartition(topic=metadata["topic"], partition=metadata["partition"])
#     consumer.seek(tp, metadata["offset"])
#     return await consumer.getone(tp)



async def startup():
    print('Ready to go')


async def startup():
    print('Ready to go')

app = Starlette(debug=True, on_startup=[startup], on_shutdown=[startup])


@app.route("/topic/", methods=["GET"])
def get_topics(request):
    return JSONResponse({"msg": 'Hello, world!'})


# @app.route("/topic/{topic_name}", methods=["GET"])
# def get_topic_schema(request):
#     topic_name = request.path_params["topic_name"]
#     return JSONResponse({topic_name: 'Hello, world!'})


# @app.route("/topic/{topic_name}/@send", methods=["POST"])
# @app.route("/topic/{topic_name}/@send/{id}", methods=["POST"])
# async def send(request):

#     topic_name = request.path_params["topic_name"]
#     agent = service_registry[topic_name]
#     data = await request.json()

#     try:
#         event = agent.model(**data)
#     except:
#         raise HTTPException(512, detail="invalid payload")

#     event_id = request.path_params.get("id") or str(uuid.uuid4())
#     sent = await agent.send(key=event_id, value=event)
#     offset_db[event_id] = dict(sent._asdict())

