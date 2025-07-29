import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from loguru import logger


def write_chunk_to_delta_table(chunk: pa.RecordBatch, index: int, output_path: str) -> None:
    """Writes a chunk to the output delta lake table.

    Args:
        chunk (pa.RecordBatch): The chunk to write
        index (int): The position of the chunk, to control `append` or `overwrite` mode
        output_path (str): The output delta table path
    """

    mode = "overwrite" if index == 0 else "append"

    write_deltalake(table_or_uri=output_path, data=chunk, mode=mode, schema_mode="overwrite")


def process(input_path: str, output_path: str, batch_size: int) -> pa.RecordBatch:
    """Process Delta table in chunks.

    Args:
        input_path (str): Path/to the delta table
        output_path (str): Path/to the output delta table
        batch_size (int): The size of the chunks

    Yields:
        pa.RecordBatch: The processed chunk
    """

    dt = DeltaTable(table_uri=input_path, storage_options={"timeout": "3600s"})

    pa_dataset = dt.to_pyarrow_dataset()

    for i, chunk in enumerate(pa_dataset.to_batches(batch_size=batch_size)):
        logger.info(f"Processing chunk {i}...")

        # Simulate a dummy processing
        chunk_df = chunk.to_pandas()
        chunk_df["avg_embeddings"] = chunk_df.apply(lambda row: row["vector"].mean(), axis=1)
        processed_chunk = pa.RecordBatch.from_pandas(chunk_df)

        write_chunk_to_delta_table(
            processed_chunk, i, output_path
        )  # commits new version for every chunk, but for this PoC is OK


def main() -> None:  # noqa: D103
    input_path = (
        "s3://ml-rd-ml-datasets/generateVectorEmbed/Qwen3-Embedding-0.6B/miracl/fr/vector_corpus/"
    )
    batch_size = 25000  # ~100MB on disk

    output_path = "s3://mvp-mlops-platform/poc-multi-instance-data-prep-shards-batch-writes-delta/"

    process(input_path, output_path, batch_size)


if __name__ == "__main__":
    main()
