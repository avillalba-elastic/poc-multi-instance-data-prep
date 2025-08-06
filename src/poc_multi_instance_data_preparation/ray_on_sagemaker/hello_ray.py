import time
from typing import Any

import ray
from loguru import logger


@ray.remote
def f() -> Any:  # noqa: D103
    logger.info("Getting node IDS...")
    time.sleep(0.01)
    return ray.runtime_context.get_runtime_context().get_node_id()


node_ids = set(ray.get([f.remote() for _ in range(1000)]))
logger.info(f"Node IDs in the Ray cluster: {node_ids}")
