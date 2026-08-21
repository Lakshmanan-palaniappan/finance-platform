from pyspark.sql import SparkSession

from finance_bundle.common.config import settings

from finance_bundle.common.paths import (
    LOGIN_ACTIVITY_INPUT_PATH,
    LOGIN_ACTIVITY_SCHEMA_PATH,
)

from finance_bundle.schemas.login_activity_schema import (
    LOGIN_ACTIVITY_SCHEMA_HINTS,
)


# ==========================================================
# Login Activity Auto Loader
# ==========================================================

def read_login_activity_stream():

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
            LOGIN_ACTIVITY_SCHEMA_PATH,
        )

        # --------------------------------------------------
        # Schema hints
        # --------------------------------------------------

        .option(
            "cloudFiles.schemaHints",
            LOGIN_ACTIVITY_SCHEMA_HINTS,
        )

        # --------------------------------------------------
        # Schema evolution
        # --------------------------------------------------

        .option(
            "cloudFiles.schemaEvolutionMode",
            settings.SCHEMA_EVOLUTION,
        )

        # --------------------------------------------------
        # Rescue unexpected columns/data
        # --------------------------------------------------

        .option(
            "rescuedDataColumn",
            "_rescued_data",
        )

        # --------------------------------------------------
        # Source
        # --------------------------------------------------

        .load(
            LOGIN_ACTIVITY_INPUT_PATH
        )
    )