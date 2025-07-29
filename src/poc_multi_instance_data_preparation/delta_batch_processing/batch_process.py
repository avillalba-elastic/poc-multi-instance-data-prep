import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake
from loguru import logger


def process(input_file: str, batch_size: int) -> pa.RecordBatch:
    """Process Delta table in chunks.

    Args:
        input_file (str): Path/to the delta table
        batch_size (int): The size of the chunks

    Yields:
        pa.RecordBatch: The processed chunk
    """

    dt = DeltaTable(table_uri=input_file, storage_options={"timeout": "3600s"})

    pa_dataset = dt.to_pyarrow_dataset()

    for i, chunk in enumerate(pa_dataset.to_batches(batch_size=batch_size)):
        logger.info(f"Processing chunk {i}")

        # Simulate a dummy processing
        means = pc.list_mean(chunk.column("vector"))
        processed_chunk = chunk.append_column("avg_embeddings", means)

        yield processed_chunk


def main() -> None:  # noqa: D103
    input_path = (
        "s3://ml-rd-ml-datasets/generateVectorEmbed/Qwen3-Embedding-0.6B/miracl/fr/vector_corpus/"
    )
    batch_size = 25000  # ~100MB on disk

    output_path = "s3://mvp-mlops-platform/poc-multi-instance-data-prep-shards-batch-writes-delta/"

    schema = pa.schema(
        [
            ("doc_id", pa.string()),
            ("vector", pa.list_(pa.float32(), 1024)),
        ]
    )

    reader = pa.RecordBatchReader.from_batches(schema, process(input_path, batch_size=batch_size))

    write_deltalake(
        output_path,
        reader,
        mode="overwrite",
        configuration={"delta.targetFileSize": str(1000 * 1024 * 1024)},
    )


if __name__ == "__main__":
    main()
