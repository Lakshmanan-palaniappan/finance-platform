from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    current_timestamp,
    current_date,
    regexp_extract,
    expr,
)

from finance_bundle.common.config import settings

from finance_bundle.common.paths import (
    CUSTOMER_INPUT_PATH,
    CUSTOMER_SCHEMA_PATH,
)

from finance_bundle.schemas.customer_schema import (
    CUSTOMER_SCHEMA_HINTS,
)


def read_customer_data():

    spark = SparkSession.getActiveSession()

    if spark is None:
        raise RuntimeError(
            "No active Spark session found."
        )

    df = (
        spark.readStream
        .format(settings.AUTOLOADER)

        # -------------------------------
        # File Format
        # -------------------------------
        .option(
            "cloudFiles.format",
            settings.FILE_FORMAT,
        )

        .option(
            "header",
            settings.HEADER,
        )

        # -------------------------------
        # Incremental Tracking
        # -------------------------------
        .option(
            "cloudFiles.schemaLocation",
            CUSTOMER_SCHEMA_PATH,
        )

        # -------------------------------
        # Schema Evolution
        # -------------------------------
        .option(
            "cloudFiles.schemaHints",
            CUSTOMER_SCHEMA_HINTS,
        )

        .option(
            "cloudFiles.schemaEvolutionMode",
            settings.SCHEMA_EVOLUTION,
        )

        # -------------------------------
        # Corrupt Records
        # -------------------------------
        .option(
            "rescuedDataColumn",
            "_rescued_data",
        )

        # -------------------------------
        # Bad Files
        # -------------------------------
        .option(
            "badRecordsPath",
            f"{CUSTOMER_SCHEMA_PATH}/bad_records"
        )

        # -------------------------------
        # Source
        # -------------------------------
        .load(CUSTOMER_INPUT_PATH)
    )

    # -----------------------------------
    # Metadata Columns
    # -----------------------------------

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
            "pipeline_run_id",
            expr("uuid()"),
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