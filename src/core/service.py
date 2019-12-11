class Registry(dict):
    def __setitem__(self, key, value):
        if key in self:
            raise Exception("Duplicated key")
        super().__setitem__(key, value)


service_registry = Registry()
offset_db = Registry()
consumer_registry = Registry()


# app = None


# async def get_from_kafka(consumer, metadata, default=None):
#     tp = TopicPartition(topic=metadata["topic"], partition=metadata["partition"])
#     consumer.seek(tp, metadata["offset"])
#     return await consumer.getone(tp)


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


# @app.route("/topic/{topic_name}/{id}", methods=["GET"])
# async def get(request):
#     topic_name = request.path_params["topic_name"]
#     event_id = request.path_params["id"]
#     try:
#         metadata = offset_db[event_id]
#     except KeyError:
#         raise HTTPException(404)
#     agent = service_registry[topic_name]
#     result = await get_from_kafka(agent.service_consumer, metadata)
