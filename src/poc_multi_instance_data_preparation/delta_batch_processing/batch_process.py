import argparse
import time

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from loguru import logger


def write_chunk_to_delta_table(batch: pa.Table, index: int, output_path: str) -> None:
    """Writes a batch to the output delta lake table.

    Args:
        batch (pa.Table): The batch to write
        index (int): The position of the batch, to control `append` or `overwrite` mode
        output_path (str): The output delta table path
    """

    mode = "overwrite" if index == 0 else "append"

    write_deltalake(table_or_uri=output_path, data=batch, mode=mode)


def process(input_path: str, output_path: str, batch_size: int) -> pa.RecordBatch:
    """Process Delta table in chunks.

    Args:
        input_path (str): Path/to the delta table
        output_path (str): Path/to the output delta table
        batch_size (int): The size of the chunks
    """

    dt = DeltaTable(table_uri=input_path, storage_options={"timeout": "3600s"})

    pa_dataset = dt.to_pyarrow_dataset()

    for i, batch in enumerate(pa_dataset.to_batches(batch_size=batch_size)):
        logger.info(f"Processing chunk {i}...")

        batch_table = pa.Table.from_batches([batch])  # force read a batch

        # Simulate a dummy processing
        time.sleep(3)  # takes 3 seconds!

        write_chunk_to_delta_table(  # force write a batch
            batch_table, i, output_path
        )  # commits new version for every chunk, but for this PoC is OK

        logger.info(f"Processed chunk {i}")


def main() -> None:  # noqa: D103
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_size", type=int, required=True)
    args = parser.parse_args()

    logger.info("Authentication to AWS Sagemaker successfully done!")

    input_path = (
        "s3://ml-rd-ml-datasets/generateVectorEmbed/Qwen3-Embedding-0.6B/miracl/fr/vector_corpus/"
    )

    output_path = f"s3://mvp-mlops-platform/poc-multi-instance-data-prep-shards-batch-writes-delta/batch_size={args.batch_size}/"

    process(input_path, output_path, args.batch_size)


if __name__ == "__main__":
    main()
