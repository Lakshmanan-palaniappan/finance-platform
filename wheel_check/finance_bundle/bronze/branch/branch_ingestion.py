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
    BRANCH_INPUT_PATH,
    BRANCH_SCHEMA_PATH,
)
from finance_bundle.schemas.branch_schema import (
    BRANCH_SCHEMA_HINTS,
)


def read_branch_data():

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
            BRANCH_SCHEMA_PATH,
        )

        # ----------------------------------------------
        # Custom schema + schema evolution
        # ----------------------------------------------

        .option(
            "cloudFiles.schemaHints",
            BRANCH_SCHEMA_HINTS,
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

        .load(BRANCH_INPUT_PATH)
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
    )

    return df