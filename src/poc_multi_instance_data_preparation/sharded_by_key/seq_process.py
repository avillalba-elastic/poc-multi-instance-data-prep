import os
import time

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

INPUT_DIR = "/opt/ml/processing/input"
OUTPUT_DIR = "/opt/ml/processing/output"


def process(input_file: str) -> pa.Table:
    """Applies a dummy processing to the input file.

    Args:
        input_file (str): The input file path to process.

    Returns:
        pd.DataFrame: The resulting dataframe
    """

    pa_table = pq.read_table(input_file)  # force reading shard

    # Simulate a dummy processing
    time.sleep(3)  # takes 3 seconds!

    return pa_table


def main() -> None:  # noqa: D103
    # Process the shards copied to current instance *sequentially*
    for i, file in enumerate(os.listdir(INPUT_DIR)):
        logger.info(f"Processing shard # {i}: {file}")
        file_path = os.path.join(INPUT_DIR, file)
        table_out = process(file_path)

        output_file_path = os.path.join(OUTPUT_DIR, f"processed_{file}")
        pq.write_table(table_out, output_file_path)  # force writing the shard


if __name__ == "__main__":
    main()
