import time
from typing import Any

import ray
from loguru import logger

from poc_multi_instance_data_preparation.ray_on_sagemaker.sagemaker_ray_helper import RayHelper

ray_helper = RayHelper()
ray_helper.start_ray()


# You can set limits on the vCPUs to use per task (default: 1), RAM memory per task,
# and num GPUs per task
@ray.remote
def f() -> Any:  # noqa: D103
    logger.info("Getting node IDS...")
    time.sleep(0.01)
    return ray.runtime_context.get_runtime_context().get_node_id()


def main() -> None:  # noqa: D103
    node_ids = set(ray.get([f.remote() for _ in range(1000)]))
    logger.info(f"Node IDs in the Ray cluster: {node_ids}")


if __name__ == "__main__":
    main()
