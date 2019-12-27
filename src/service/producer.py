import aiokafka
import jsonschema
from contextvars import ContextVar


class Producer(aiokafka.AIOKafkaProducer):

    @property
    def running(self):
        return getattr(self, "_running", False)

    async def start(self):
        await super().start()
        self._running = True

    async def stop(self):
        await super().stop()
        self._running = False

    async def send_json(self, topic, value=None, schema=None, **kwargs):
        assert schema, "Invalid JSON schema"
        jsonschema.validate(value, schema)
        if not self.running:
            await self.start()
        sent = await self.send(topic, value=value, **kwargs)
        return await sent
