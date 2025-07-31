import argparse

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake
from loguru import logger

from poc_multi_instance_data_preparation.utils import auth


def write_chunk_to_delta_table(batch: pa.RecordBatch, index: int, output_path: str) -> None:
    """Writes a batch to the output delta lake table.

    Args:
        batch (pa.RecordBatch): The batch to write
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
        logger.info(f"Processing batch {i}: {batch.num_rows} rows")

        # Simulate a dummy processing
        vec_tensor = pc.cast(batch["vector"], pa.list_(pa.float32()))
        vec_array = vec_tensor.to_pylist()
        multiplied_array = [[x * 2 for x in sublist] for sublist in vec_array]
        new_column = pa.array(multiplied_array, type=batch["vector"].type)
        processed_batch = batch.append_column("vector_x2", new_column)

        write_chunk_to_delta_table(
            processed_batch, i, output_path
        )  # commits new version for every chunk, but for this PoC is OK

        logger.info(f"Batch {i} processed and saved to disk")


def main() -> None:  # noqa: D103
    parser = argparse.ArgumentParser()
    parser.add_argument("--local_mode", type=bool, required=False, default=False)
    parser.add_argument("--batch_size", type=int, required=True)
    args = parser.parse_args()

    if args.local_mode:
        auth()

    # The input path is a copy of s3://ml-rd-ml-datasets/generateVectorEmbed/Qwen3-Embedding-0.6B/miracl/fr/vector_corpus/
    # but repartitioned in parquet files of 1GB per each.
    input_path = "s3://mvp-mlops-platform/poc-multi-instance-data-prep-repartitioned-delta/"

    output_path = f"s3://mvp-mlops-platform/poc-multi-instance-data-prep-seq_delta_outputs/batch_size={args.batch_size}/"

    process(input_path, output_path, args.batch_size)


if __name__ == "__main__":
    main()
