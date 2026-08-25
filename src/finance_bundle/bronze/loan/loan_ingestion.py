from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    current_date,
    current_timestamp,
    regexp_extract,
    expr,
)

from finance_bundle.common.config import settings

from finance_bundle.common.paths import (
    LOAN_INPUT_PATH,
    LOAN_SCHEMA_PATH,
)

from finance_bundle.schemas.loan_schema import (
    LOAN_SCHEMA_HINTS,
)


def read_loan_data():

    spark = SparkSession.getActiveSession()

    if spark is None:
        raise RuntimeError(
            "No active Spark session found."
        )

    df = (
        spark.readStream
        .format(settings.AUTOLOADER)

        # --------------------------------------------------
        # File Format
        # --------------------------------------------------

        .option(
            "cloudFiles.format",
            settings.FILE_FORMAT,
        )

        .option(
            "header",
            settings.HEADER,
        )

        # --------------------------------------------------
        # Auto Loader Schema Location
        # --------------------------------------------------

        .option(
            "cloudFiles.schemaLocation",
            LOAN_SCHEMA_PATH,
        )

        # --------------------------------------------------
        # Schema Hints
        # --------------------------------------------------

        .option(
            "cloudFiles.schemaHints",
            LOAN_SCHEMA_HINTS,
        )

        # --------------------------------------------------
        # Schema Evolution
        # --------------------------------------------------

        .option(
            "cloudFiles.schemaEvolutionMode",
            settings.SCHEMA_EVOLUTION,
        )

        # --------------------------------------------------
        # Rescued Data
        # --------------------------------------------------

        .option(
            "rescuedDataColumn",
            "_rescued_data",
        )

        # --------------------------------------------------
        # Bad Records
        # --------------------------------------------------

        .option(
            "badRecordsPath",
            f"{LOAN_SCHEMA_PATH}/bad_records",
        )

        # --------------------------------------------------
        # Source
        # --------------------------------------------------

        .load(
            LOAN_INPUT_PATH
        )
    )

    # ======================================================
    # Metadata
    # ======================================================

    return (
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