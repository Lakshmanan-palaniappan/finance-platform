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


def read_card_cdc_data():

    spark = SparkSession.getActiveSession()

    if spark is None:
        raise RuntimeError(
            "No active Spark session found."
        )

    df = (
        spark.readStream
        .format(settings.AUTOLOADER)

        .option(
            "cloudFiles.format",
            settings.FILE_FORMAT,
        )

        .option(
            "header",
            settings.HEADER,
        )

        .option(
            "cloudFiles.schemaLocation",
            SchemaLocation.CARD_CDC,
        )

        .option(
            "cloudFiles.schemaEvolutionMode",
            "addNewColumns",
        )

        .option(
            "rescuedDataColumn",
            "_rescued_data",
        )

        .load(CDC.CARD)
    )

    # ==========================================================
    # Normalize column names
    # ==========================================================

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

    # ==========================================================
    # Normalize CDC values
    # ==========================================================

    df = (
        df
        .withColumn(
            "entity",
            lower(trim(col("entity"))),
        )
        .withColumn(
            "operation",
            lower(trim(col("operation"))),
        )
        .withColumn(
            "card_id",
            trim(col("card_id")),
        )
        .withColumn(
            "customer_id",
            trim(col("customer_id")),
        )
        .withColumn(
            "account_id",
            trim(col("account_id")),
        )
        .withColumn(
            "event_id",
            trim(col("event_id")),
        )
        .withColumn(
            "batch_id",
            trim(col("batch_id")),
        )
        .withColumn(
            "source_system",
            trim(col("source_system")),
        )
        .withColumn(
            "card_type",
            trim(col("card_type")),
        )
        .withColumn(
            "network",
            trim(col("network")),
        )
    )

    # ==========================================================
    # Metadata
    # ==========================================================

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