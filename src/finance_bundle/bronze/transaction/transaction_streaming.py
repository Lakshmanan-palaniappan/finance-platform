from pyspark.sql import SparkSession

from finance_bundle.common.config import settings

from finance_bundle.common.paths import (
    TRANSACTION_INPUT_PATH,
    TRANSACTION_SCHEMA_PATH,
)

from finance_bundle.schemas.transaction_schema import (
    TRANSACTION_SCHEMA_HINTS,
)


# ==========================================================
# Transaction Auto Loader Streaming Reader
# ==========================================================

def read_transaction_stream():

    spark = SparkSession.getActiveSession()

    if spark is None:
        raise RuntimeError(
            "No active Spark session found."
        )

    return (
        spark.readStream
        .format(
            settings.AUTOLOADER
        )

        # --------------------------------------------------
        # Source format
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
        # Auto Loader schema location
        # --------------------------------------------------

        .option(
            "cloudFiles.schemaLocation",
            TRANSACTION_SCHEMA_PATH,
        )

        # --------------------------------------------------
        # Schema hints
        # --------------------------------------------------

        .option(
            "cloudFiles.schemaHints",
            TRANSACTION_SCHEMA_HINTS,
        )

        # --------------------------------------------------
        # Schema evolution
        # --------------------------------------------------

        .option(
            "cloudFiles.schemaEvolutionMode",
            settings.SCHEMA_EVOLUTION,
        )

        # --------------------------------------------------
        # Rescued data
        # --------------------------------------------------

        .option(
            "rescuedDataColumn",
            "_rescued_data",
        )

        # --------------------------------------------------
        # Source
        # --------------------------------------------------

        .load(
            TRANSACTION_INPUT_PATH
        )
    )