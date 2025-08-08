import argparse

import pyarrow as pa
import ray
from loguru import logger

from poc_multi_instance_data_preparation.ray_on_sagemaker.sagemaker_ray_helper import RayHelper
from poc_multi_instance_data_preparation.utils import auth


def transform(batch: pa.Table) -> pa.Table:
    """Applies a dummy transformation to a batch of the dataset.

    Ray automatically parallelizes this transformation across the Ray cluster.

    Args:
        batch (pa.Table): Input batch.

    Returns:
        pa.Table: Transformed batch
    """

    # Simulate a dummy processing
    vec_array = batch["vector"].to_pylist()
    multiplied_array = [[x * 2 for x in sublist] for sublist in vec_array]
    new_column = pa.array(multiplied_array, type=batch.schema.field("vector").type)
    pa_table = batch.append_column("vector_x2", new_column)

    return pa_table


def main() -> None:  # noqa: D103
    parser = argparse.ArgumentParser()
    parser.add_argument("--local_mode", type=bool, required=False, default=False)
    args = parser.parse_args()

    if args.local_mode:
        auth()  # credentials must be configured before initializing Ray
        raise NotImplementedError("Local mode has not been tested yet")

    ray_helper = RayHelper()
    ray_helper.start_ray()

    INPUT_PATH = "/opt/ml/processing/input"  # mapped through sagemaker processing inputs
    OUTPUT_PATH = "/opt/ml/processing/output"  # mapped through sagemaker processing outputs

    batch_size = (
        220224 / 8
    )  # to fairly compare it with Sagemaker sharding and Delta sequential batching  # TODO: Vary

    logger.info(f"Reading data at {INPUT_PATH}")
    ds = ray.data.read_parquet(
        f"local://{INPUT_PATH}"
    )  # TODO: Parquet or open table format <-> Spark <-> Ray?

    logger.info(f"Dataset at {INPUT_PATH} successfully loaded! Processing...")
    ds_transformed = ds.map_batches(
        fn=transform,
        batch_size=batch_size,
        batch_format="pyarrow",
        num_cpus=1,  # cpus reserved per batch, Ray parallelizes using all available vCPUs
    )

    logger.info(f"Dataset at {INPUT_PATH} successfully processed! Saving...")
    ds_transformed.write_parquet(f"local://{OUTPUT_PATH}")
    logger.info(f"Dataset at {INPUT_PATH} successfully saved!")


if __name__ == "__main__":
    main()
