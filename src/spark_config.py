"""
Spark session factory with Delta Lake and Kafka connector support.

Why a separate file from config.py:
- config.py holds values (strings, numbers) — no Spark imports
- spark_config.py builds the SparkSession — heavy PySpark dependency
- Every Spark script calls get_spark_session() instead of duplicating 10 config lines

JAR packages are pulled automatically on first run via spark.jars.packages —
no manual download. First startup takes 1–2 minutes while Maven resolves deps.
"""

from pyspark.sql import SparkSession

from config import (
    SPARK_APP_NAME,
    SPARK_DRIVER_MEMORY,
    SPARK_MASTER,
    SPARK_SHUFFLE_PARTITIONS,
)


def get_spark_session(app_name: str | None = None) -> SparkSession:
    """
    Create or reuse a SparkSession configured for Structured Streaming + Delta.

    Package versions must match PySpark version (3.5.1) and Scala binary (2.12):
      - spark-sql-kafka-0-10  → read/write Kafka topics as DataFrames
      - delta-spark           → ACID table format for streaming sinks (Hour 4+)
    """
    return (
        SparkSession.builder
        .appName(app_name or SPARK_APP_NAME)
        .master(SPARK_MASTER)
        # Maven coordinates — downloaded once, cached in ~/.ivy2/
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "io.delta:delta-spark_2.12:3.1.0",
        )
        # Register Delta SQL extensions so .format("delta") works in writeStream
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Local dev tuning — avoid 200 shuffle partitions on tiny datasets
        .config("spark.sql.shuffle.partitions", str(SPARK_SHUFFLE_PARTITIONS))
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .getOrCreate()
    )
