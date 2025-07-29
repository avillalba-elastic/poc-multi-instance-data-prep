import os

import pandas as pd
from loguru import logger

INPUT_DIR = "/opt/ml/processing/input"
OUTPUT_DIR = "/opt/ml/processing/output"


def process(input_file: str) -> pd.DataFrame:
    """Applies a dummy processing to the input file.

    Args:
        input_file (str): The input file path to process.

    Returns:
        pd.DataFrame: The resulting dataframe
    """

    df = pd.read_parquet(input_file)

    # Simulate a dummy processing
    df["avg_embeddings"] = df.apply(lambda row: row["vector"].mean(), axis=1)

    return df


def main() -> None:  # noqa: D103
    # Process the shards copied to current instance sequentially
    for i, file in enumerate(os.listdir(INPUT_DIR)):
        logger.info(f"Processing shard # {i}: {file}")
        file_path = os.path.join(INPUT_DIR, file)
        df_out = process(file_path)

        output_file_path = os.path.join(OUTPUT_DIR, f"processed_{file}")
        df_out.to_parquet(output_file_path, index=False)


if __name__ == "__main__":
    main()
