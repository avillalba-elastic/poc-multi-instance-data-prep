import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main() -> None:  # noqa: D103
    spark = SparkSession.builder.appName("MySparkApp").getOrCreate()

    df = spark.createDataFrame(
        data=[[2022, "tiger"], [2023, "rabbit"], [2024, "dragon"]], schema=["year", "animal"]
    )

    # show the first few rows of the dataframe
    df.show(5)

    # Print the schema
    df.printSchema()

    # Add a new column
    df.withColumn(
        "from_fantasy_fiction", f.when(f.col("animal") == "dragon", True).otherwise(False)
    )


if __name__ == "__main__":
    main()
