from pyspark.sql import SparkSession

from finance_bundle.common.config import settings
from finance_bundle.common.paths import (
    ATM_TRANSACTION_INPUT_PATH,
    ATM_TRANSACTION_SCHEMA_PATH,
)

from finance_bundle.schemas.atm_transaction_schema import (
    ATM_TRANSACTION_SCHEMA_HINTS,
)


def read_atm_transaction_stream():

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
            ATM_TRANSACTION_SCHEMA_PATH,
        )

        # ----------------------------------------------
        # Schema hints
        # ----------------------------------------------

        .option(
            "cloudFiles.schemaHints",
            ATM_TRANSACTION_SCHEMA_HINTS,
        )

        # ----------------------------------------------
        # Schema evolution
        # ----------------------------------------------

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
        # Streaming source
        # ----------------------------------------------

        .load(
            ATM_TRANSACTION_INPUT_PATH
        )
    )

    return df