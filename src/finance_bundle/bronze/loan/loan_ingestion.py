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
            LOAN_SCHEMA_PATH,
        )

        .option(
            "cloudFiles.schemaHints",
            LOAN_SCHEMA_HINTS,
        )

        .option(
            "cloudFiles.schemaEvolutionMode",
            settings.SCHEMA_EVOLUTION,
        )

        .option(
            "rescuedDataColumn",
            "_rescued_data",
        )

        .load(LOAN_INPUT_PATH)
    )

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
    )