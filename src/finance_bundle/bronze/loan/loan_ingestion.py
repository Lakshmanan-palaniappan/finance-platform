from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    current_date,
    input_file_name,
    regexp_extract,
    expr
)

from finance_bundle.schemas.loan_schema import loan_schema
from finance_bundle.common.config import settings
from finance_bundle.common.paths import (
    LOAN_INPUT_PATH,
    LOAN_SCHEMA_PATH
)


def read_loan_data():

    spark = SparkSession.getActiveSession()

    df = (
        spark.readStream
            .format(settings.AUTOLOADER)
            .option("cloudFiles.format", settings.FILE_FORMAT)
            .option("header", settings.HEADER)
            .option("cloudFiles.schemaLocation", LOAN_SCHEMA_PATH)
            .option(
                "cloudFiles.schemaEvolutionMode",
                settings.SCHEMA_EVOLUTION
            )
            .option("rescuedDataColumn", "_rescued_data")
            .schema(loan_schema)
            .load(LOAN_INPUT_PATH)
    )

    df = (
        df
            .withColumn(
                "ingestion_timestamp",
                current_timestamp()
            )
            .withColumn(
                "ingestion_date",
                current_date()
            )
            .withColumn(
                "pipeline_run_id",
                expr("uuid()")
            )
            .withColumn(
                "source_file",
                input_file_name()
            )
            .withColumn(
                "file_name",
                regexp_extract(
                    input_file_name(),
                    "([^/]+$)",
                    1
                )
            )
    )

    return df