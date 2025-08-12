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
        # We have a sample of a delta table locally, for debugging purposes
        INPUT_DELTA_TABLE_PATH = "data/"
        OUTPUT_DELTA_TABLE_PATH = "outputs/"
    else:
        # Point to the whole dataset at s3. You could also load this locally, after auth to AWS.
        # However you are going to run into OOO issues if your dataset does not fit in your local
        # resources.
        # We use the Hadopp S3A protocol to access S3 from Spark
        INPUT_DELTA_TABLE_PATH = (
            "s3a://mvp-mlops-platform/poc-multi-instance-data-prep-repartitioned-delta/"
        )
        OUTPUT_DELTA_TABLE_PATH = f"s3a://mvp-mlops-platform/poc-multi-instance-data-prep-delta-pyspark_outputs/instance_count={args.n_instances}"

    # Dependencies for Spark. See: https://aws.amazon.com/blogs/machine-learning/load-and-transform-data-from-delta-lake-using-amazon-sagemaker-studio-and-apache-spark/
    # Delta Lake <-> Spark compability matrix at: https://docs.delta.io/latest/releases.html.
    # Delta Lake installation: https://docs.delta.io/latest/quick-start.html#set-up-apache-spark-with-delta-lake
    # In Sagemaker, we use a prebuilt Docker image with Spark = 3.5.0 and Scala 2.12
    # (see docker bake).
    packages = ",".join(
        [
            # Delta Lake connector for Spark, compiled for Scala 2.12 and Delta Lake = 3.2.0,
            # compatible with Spark 3.5.0
            "io.delta:delta-spark_2.12:3.2.0",
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
        .config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )  # to extend Spark SQL with Delta Lake functions
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",  # Use Delta Lake as default catalog
        )
        # The dataset is partitioned in Parquet files of ~1GB each. Thus, we need to give enough
        # memory to the Spark JVM. Each instance has 32GB and 8 vCPU, so 8 cores per executor.
        .config("spark.executor.memory", "12g")
        .config("spark.executor.memoryOverhead", "2g")
        .config("spark.driver.memory", "12g")
        .config("spark.executor.cores", "8")
        .getOrCreate()
    )

    logger.info(f"Reading Delta table from: {INPUT_DELTA_TABLE_PATH}")
    df = spark.read.format("delta").load(INPUT_DELTA_TABLE_PATH)

    logger.info(f"Processing the dataset at: {INPUT_DELTA_TABLE_PATH};")
    df_transformed = df.withColumn("vector_x2", vector_x2_udf(df["vector"]))

    logger.info(f"Writing processed dataset to {OUTPUT_DELTA_TABLE_PATH}")
    df_transformed.write.format("delta").mode("overwrite").save(OUTPUT_DELTA_TABLE_PATH)

    logger.info("Processing completed")


if __name__ == "__main__":
    main()
