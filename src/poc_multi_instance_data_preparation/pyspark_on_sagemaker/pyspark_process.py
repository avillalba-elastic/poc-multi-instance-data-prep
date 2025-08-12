import argparse
from collections.abc import Iterable

import pandas as pd
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, FloatType, StringType, StructField, StructType


def transform(iterator: Iterable[pd.DataFrame]) -> Iterable[pd.DataFrame]:
    """Apply a dummy processing (x2 the embeddings).

    Args:
        iterator (Iterable[pd.DataFrame]): The input batch

    Yields:
        Iterable[pd.DataFrame]: The transformed batch
    """

    # This is the most similar way to Ray for applying the same dummy processing.
    # There are alternatives at Spark, like using pandas_udf or even `expr`. But
    # we want to be "fair".

    for pdf in iterator:
        vec_array = pdf["vector"].tolist()
        multiplied_array = [[x * 2 for x in sublist] for sublist in vec_array]
        pdf["vector_x2"] = multiplied_array
        yield pdf


def main() -> None:  # noqa: D103
    parser = argparse.ArgumentParser()
    parser.add_argument("--local_mode", type=bool, required=False, default=False)
    parser.add_argument("--n_instances", type=int, required=True)
    args = parser.parse_args()

    if args.local_mode:
        # We have a sample of a delta table locally, for debugging purposes
        INPUT_DELTA_TABLE_PATH = "data/"
        OUTPUT_DELTA_TABLE_PATH = "outputs/"
    else:
        # Point to the whole dataset at s3. You could also load this locally, after auth to AWS.
        # However you are going to run into OOO issues if your dataset does not fit in your local
        # resources.
        INPUT_DELTA_TABLE_PATH = (
            "s3a://mvp-mlops-platform/poc-multi-instance-data-prep-repartitioned-delta/"
        )
        OUTPUT_DELTA_TABLE_PATH = f"s3a://mvp-mlops-platform/poc-multi-instance-data-prep-delta-pyspark_outputs/instance_count={args.n_instances}"

    # See: https://docs.delta.io/latest/quick-start.html#set-up-apache-spark-with-delta-lake
    # and https://docs.delta.io/latest/releases.html.
    # Also see: https://aws.amazon.com/blogs/machine-learning/load-and-transform-data-from-delta-lake-using-amazon-sagemaker-studio-and-apache-spark/
    # In the `pyspark` Docker container, we use Scala 2.12 and Spark 3.5.0:
    packages = ",".join(
        [
            "io.delta:delta-spark_2.12:3.2.0",
            "org.apache.hadoop:hadoop-aws:3.4.0",
            "com.amazonaws:aws-java-sdk-bundle:1.12.329",
        ]
    )

    spark = (
        SparkSession.builder.appName("MyApp")
        .config("spark.jars.packages", packages)
        .config(
            "fs.s3a.aws.credentials.provider", "com.amazonaws.auth.ContainerCredentialsProvider"
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .getOrCreate()
    )

    logger.info(f"Reading Delta table from: {INPUT_DELTA_TABLE_PATH}")
    df = spark.read.format("delta").load(INPUT_DELTA_TABLE_PATH)

    # To fairly compare Ray vs. Spark in terms of performance, we need to handle the
    # same batch size. However, there are more aspects to take into account to choose
    # between Spark or Ray.
    total_rows = df.count()
    batch_size = 220224 / 8  # same as Ray
    num_partitions = int(max(1, total_rows // batch_size))
    df = df.repartition(num_partitions)

    logger.info(
        f"Processing the dataset at: {INPUT_DELTA_TABLE_PATH};"
        f"# of partitions = {num_partitions};"
        f"batch size = {batch_size}"
    )
    schema = StructType(
        [
            StructField("doc_id", StringType()),
            StructField("vector", ArrayType(FloatType())),
            StructField("vector_x2", ArrayType(FloatType())),
        ]
    )
    df_transformed = df.mapInPandas(transform, schema=schema)

    logger.info(f"Writing processed dataset to {OUTPUT_DELTA_TABLE_PATH}")
    df_transformed.write.format("delta").mode("overwrite").save(OUTPUT_DELTA_TABLE_PATH)

    logger.info("Processing completed")


if __name__ == "__main__":
    main()
