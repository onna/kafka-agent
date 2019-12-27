import json
import logging
import os
from copy import deepcopy


logger = logging.getLogger(__name__)


default_settings = {
    "shared": {
        "bootstrap_servers": ["localhost:9200"]
    },
    "producer": {},
    "consumer": {
        "max_partition_fetch_bytes": 10240,
        "metadata_max_age_ms": 10000
    },
    "service": {
        "host": "0.0.0.0",
        "port": 7000,
        "debug": False,
        "reload": False,
        "timeout_keep_alive": 20,
        "proxy_headers": True,
        "log_level": "info"
    }
}


def load_configuration_file(configuration):
    with configuration:
        try:
            settings = json.load(configuration)
        except json.decoder.JSONDecodeError:
            logger.warning(
                "Could not parse json configuration {}".format(
                    configuration
                )
            )
            raise
    return settings
