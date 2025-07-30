import argparse
import os
import time

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from poc_multi_instance_data_preparation.utils import auth, log_memory


def process(input_file: str) -> pa.Table:
    """Applies a dummy processing to the input file.

    Args:
        input_file (str): The input file path to process.

    Returns:
        pd.DataFrame: The resulting dataframe
    """

    pa_table = pq.read_table(input_file)  # force reading shard

    # Simulate a dummy processing
    vec_array = pa_table["vector"].to_pylist()
    multiplied_array = [[x * 2 for x in sublist] for sublist in vec_array]
    new_column = pa.array(multiplied_array, type=pa_table.schema.field("vector").type)
    pa_table = pa_table.append_column("vector_x2", new_column)

    return pa_table


def main() -> None:  # noqa: D103
    parser = argparse.ArgumentParser()
    parser.add_argument("--local_mode", type=bool, required=False, default=False)
    args = parser.parse_args()

    if args.local_mode:
        auth()
        input_dir = "data"
        output_dir = "outputs"
    else:  # in Sagemaker, we mount these directories via ProcessingInputs and ProcessingOutputs
        input_dir = "/opt/ml/processing/input"
        output_dir = "/opt/ml/processing/output"

    # Process the shards copied to current instance *sequentially*
    for i, file in enumerate(os.listdir(input_dir)):
        logger.info(f"Processing shard # {i}: {file}")
        log_memory()
        start_time = time.time()

        file_path = os.path.join(input_dir, file)
        table_out = process(file_path)

        logger.info(f"Processing applied to shard {i}")
        log_memory()

        output_file_path = os.path.join(output_dir, f"processed_{file}")
        pq.write_table(table_out, output_file_path)

        logger.info(f"Shard {i} saved to disk")
        logger.info(f"Shard {i} processing time: {time.time() - start_time:.2f}s")


if __name__ == "__main__":
    main()
