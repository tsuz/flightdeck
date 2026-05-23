"""Pass-through of KAFKA_* environment variables into a Kafka client config dict.

KAFKA_FOO_BAR_BAZ becomes foo.bar.baz. Application-level variables that share
the KAFKA_ prefix but are not Kafka client config are skipped (input/output
topic, consumer group). These will be renamed without the prefix in the future.
"""

import os

_PREFIX = "KAFKA_"

_SKIP = {
    "KAFKA_INPUT_TOPIC",
    "KAFKA_OUTPUT_TOPIC",
    "KAFKA_CONSUMER_GROUP",
}


def kafka_env_props() -> dict[str, str]:
    """Return a config dict built from all KAFKA_* env vars."""
    out: dict[str, str] = {}
    for key, value in os.environ.items():
        if not key.startswith(_PREFIX) or key in _SKIP:
            continue
        mapped = key[len(_PREFIX):].lower().replace("_", ".")
        out[mapped] = value
    return out
