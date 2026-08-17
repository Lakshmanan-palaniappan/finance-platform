from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    current_date,
    current_timestamp,
    regexp_extract,
    trim,
    lower,
)

from finance_bundle.common.config import settings

from finance_bundle.common.paths import (
    CDC,
    SchemaLocation,
)


def read_account_cdc_data():

    spark = SparkSession.getActiveSession()

    if spark is None:
        raise RuntimeError(
            "No active Spark session found."
        )

    df = (
        spark.readStream
        .format(settings.AUTOLOADER)

        # ==================================================
        # File Format
        # ==================================================

        .option(
            "cloudFiles.format",
            settings.FILE_FORMAT,
        )

        .option(
            "header",
            settings.HEADER,
        )

        # ==================================================
        # Schema Location
        # ==================================================

        .option(
            "cloudFiles.schemaLocation",
            SchemaLocation.ACCOUNT_CDC,
        )

        .option(
            "cloudFiles.schemaEvolutionMode",
            "addNewColumns",
        )

        # ==================================================
        # Rescued Data
        # ==================================================

        .option(
            "rescuedDataColumn",
            "_rescued_data",
        )

        # ==================================================
        # Source
        # ==================================================

        .load(CDC.ACCOUNT)
    )

    # ======================================================
    # Normalize CDC Column Names
    # ======================================================

    for column_name in df.columns:

        normalized = (
            column_name
            .strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
        )

        if normalized != column_name:

            df = df.withColumnRenamed(
                column_name,
                normalized,
            )

    # ======================================================
    # Normalize Entity
    # ======================================================

    if "entity" in df.columns:

        df = df.withColumn(
            "entity",
            lower(
                trim(
                    col("entity")
                )
            ),
        )

    # ======================================================
    # Normalize Operation
    # ======================================================

    if "operation" in df.columns:

        df = df.withColumn(
            "operation",
            lower(
                trim(
                    col("operation")
                )
            ),
        )

    # ======================================================
    # Normalize Account ID
    # ======================================================

    if "account_id" in df.columns:

        df = df.withColumn(
            "account_id",
            trim(
                col("account_id")
            ),
        )

    # ======================================================
    # Normalize Batch ID
    # ======================================================

    if "batch_id" in df.columns:

        df = df.withColumn(
            "batch_id",
            trim(
                col("batch_id")
            ),
        )

    # ======================================================
    # Metadata
    # ======================================================

    df = (
        df

        .withColumn(
            "ingestion_timestamp",
            current_timestamp(),
        )

        .withColumn(
            "ingestion_date",
            current_date(),
        )

        .withColumn(
            "source_file",
            col("_metadata.file_path"),
        )

        .withColumn(
            "file_name",
            regexp_extract(
                col("_metadata.file_path"),
                "([^/]+$)",
                1,
            ),
        )

        .withColumn(
            "file_size",
            col("_metadata.file_size"),
        )

        .withColumn(
            "file_modification_time",
            col("_metadata.file_modification_time"),
        )
    )

    return df