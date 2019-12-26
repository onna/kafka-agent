import argparse
import asyncio
import logging
import nest_asyncio
from core.const import banner
from core.utils import resolve_dotted_name
from core.component import ProducerComponent


nest_asyncio.apply()
logger = logging.getLogger(__name__)


class Producer:
    def __init__(self, model, **config):
        self.model = model
        self.config = config

    async def send(self, *agrs, **kwargs):
        if not hasattr(self, "producer"):
            self.producer = ProducerComponent(self.model, **self.config)
            await self.producer.start()
        return await self.producer.send(*agrs, **kwargs)


class CommandRunner:
    """
    kafka-agent agent --config=config.json path.to.agent.func
    kafka-agent consumer --config=config.json path.to.consumer.func
    kafka-agent producer --config=config.json core.model.BaseTopicSchema
    kafka-agent service --config=config.json --host=0.0.0.0 --port=8080
    """

    modules = ["agent", "consumer", "producer"]

    def __init__(self, description, add_help=True):
        self.parser = argparse.ArgumentParser(
            description=description, add_help=add_help
        )

        self.parser.add_argument("module")
        self.parser.add_argument("src", nargs="+")
        self.parser.add_argument("-c", "--config")

    def get_config(self, name=None):
        return {
            "producer": self.producer_config,
            "consumer": self.consumer_config,
            "agent": self.agent_config,
        }.get(name)

    @property
    def producer_config(self):
        return self.arguments.config.get("producer", {})

    @property
    def consumer_config(self):
        return self.arguments.config.get("consumer", {})

    @property
    def agent_config(self):
        return {
            "producer_config": self.producer_config,
            "consumer_config": self.consumer_config,
            **self.arguments.config.get("shared", {}),
        }

    @property
    def service_config(self):
        return self.arguments.config.get("service", {})

    async def start_component(component, **config):
        component.configure(**config)
        await component.strat()

    async def start_producer(self):
        from IPython.terminal.embed import InteractiveShellEmbed
        from traitlets.config.loader import Config

        namespace = {
            src.split(".")[-1]: resolve_dotted_name(src) for src in self.arguments.src
        }
        model = namespace[self.arguments.src[0].split(".")[-1]]

        cfg = Config()
        cfg.InteractiveShellApp.exec_lines = [f"import {self.arguments.src}"]
        ipshell = InteractiveShellEmbed(
            config=cfg, banner1=banner.format(cls=model.__name__)
        )

        ipshell(
            local_ns={
                **namespace,
                "producer": Producer(
                    namespace[self.arguments.src[0].split(".")[-1]],
                    key_serializer=lambda key: key.encode(),
                    bootstrap_servers="kafka-intra01.intra.onna.internal:9092",
                ),
            },
        )

    async def __call__(self):
        self.arguments, _ = self.parser.parse_known_args()
        if self.arguments.module not in self.modules:
            raise Exception(f"Invalid command: {self.arguments.module}")

        if self.arguments.module.lower() in ("consumer", "agent"):
            try:
                component = resolve_dotted_name(self.arguments.src)
            except ModuleNotFoundError:
                raise Exception(f"ModuleNotFoundError: {self.arguments.src}")

            config = self.get_config(name=self.arguments.module)
            await self.start_component(component, **config)
        elif self.arguments.module.lower() == "producer":
            await self.start_producer()


def run():
    command_runner = CommandRunner("KAFKA Agent cli.")
    asyncio.run(command_runner())
