import argparse

import numpy as np
import pandas as pd
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, FloatType


@pandas_udf(ArrayType(FloatType()))
def vector_x2_udf(vectors: pd.Series) -> pd.Series:  # noqa: D103
    return vectors.apply(lambda arr: (np.array(arr) * 2).tolist())


def main() -> None:  # noqa: D103
    parser = argparse.ArgumentParser()
    parser.add_argument("--local_mode", type=bool, required=False, default=False)
    parser.add_argument("--n_instances", type=int, required=True)
    args = parser.parse_args()

    if args.local_mode:
        # We have a sample of a dataset locally, for debugging purposes
        INPUT_PATH = "data/"
        OUTPUT_PATH = "outputs/"
    else:
        # Point to the whole dataset at s3. You could also load this locally, after auth to AWS.
        # However you are going to run into OOO issues if your dataset does not fit in your local
        # resources.
        INPUT_PATH = "s3a://mvp-mlops-platform/poc-multi-instance-data-prep-repartitioned-parquet/"
        OUTPUT_PATH = f"s3a://mvp-mlops-platform/poc-multi-instance-data-prep-delta-pyspark_outputs/instance_count={args.n_instances}"

    packages = ",".join(
        [
            # Hadoop module to connect to S3 using S3A protocol. Compatible with Spark 3.5.0.
            "org.apache.hadoop:hadoop-aws:3.4.0",
            # AWS SDK for Java, needed to use hadoop-aws
            "com.amazonaws:aws-java-sdk-bundle:1.12.329",
        ]
    )

    spark = (
        SparkSession.builder.appName("MyApp")
        .config(
            "spark.jars.packages", packages
        )  # dependencies to download and add to JVM classpath
        .config(
            "fs.s3a.aws.credentials.provider", "com.amazonaws.auth.ContainerCredentialsProvider"
        )  # to use Sagemaker IAM Role
        .config("spark.executor.memory", "12g")
        .config("spark.executor.memoryOverhead", "2g")
        .config("spark.driver.memory", "12g")
        .config("spark.executor.cores", "8")
        .getOrCreate()
    )

    logger.info(f"Reading input dataset from: {INPUT_PATH}")
    df = spark.read.parquet(INPUT_PATH)

    n_partitions = 256  # ~200MB per partition, 64 cores in total
    logger.info(f"Repartitioning dataset into {n_partitions} partitions")
    df = df.repartition(n_partitions)

    logger.info(f"Processing the dataset at: {INPUT_PATH};")
    df_transformed = df.withColumn("vector_x2", vector_x2_udf(df["vector"]))

    logger.info(f"Writing processed dataset to {OUTPUT_PATH}")
    df_transformed.write.mode("overwrite").parquet(OUTPUT_PATH)

    logger.info("Processing completed")


if __name__ == "__main__":
    main()
