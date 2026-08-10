from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    current_date,
    input_file_name,
    regexp_extract,
    expr,
)

from finance_bundle.common.config import settings
from finance_bundle.common.paths import (
    LOAN_INPUT_PATH,
    LOAN_SCHEMA_PATH,
)
from finance_bundle.schemas.loan_schema import LOAN_SCHEMA_HINTS


def read_loan_data():

    spark = SparkSession.getActiveSession()

    if spark is None:
        raise RuntimeError(
            "No active Spark session found."
        )

    df = (
        spark.readStream
        .format(settings.AUTOLOADER)

        # ----------------------------------------------
        # Source format
        # ----------------------------------------------

        .option(
            "cloudFiles.format",
            settings.FILE_FORMAT,
        )

        .option(
            "header",
            settings.HEADER,
        )

        # ----------------------------------------------
        # Auto Loader schema state
        # ----------------------------------------------

        .option(
            "cloudFiles.schemaLocation",
            LOAN_SCHEMA_PATH,
        )

        # ----------------------------------------------
        # Custom schema + schema evolution
        # ----------------------------------------------

        .option(
            "cloudFiles.schemaHints",
            LOAN_SCHEMA_HINTS,
        )

        .option(
            "cloudFiles.schemaEvolutionMode",
            settings.SCHEMA_EVOLUTION,
        )

        # ----------------------------------------------
        # Rescue unexpected values
        # ----------------------------------------------

        .option(
            "rescuedDataColumn",
            "_rescued_data",
        )

        # ----------------------------------------------
        # Source path
        # ----------------------------------------------

        .load(LOAN_INPUT_PATH)
    )

    # ----------------------------------------------
    # Ingestion metadata
    # ----------------------------------------------

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
            input_file_name(),
        )
        .withColumn(
            "file_name",
            regexp_extract(
                input_file_name(),
                "([^/]+$)",
                1,
            ),
        )
    )

    return df