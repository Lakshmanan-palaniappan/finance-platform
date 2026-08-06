from pyspark.shell import spark
from pyspark.sql.functions import (
    current_timestamp,
    current_date,
    input_file_name,
    regexp_extract,
    expr
)

from schemas.loan_schema import loan_schema
from common.paths import LOAN_INPUT_PATH, LOAN_SCHEMA_PATH

def read_loan_data():

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", LOAN_SCHEMA_PATH)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(loan_schema)
        .load(LOAN_INPUT_PATH)
    )

    df = (
        df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("ingestion_date", current_date())
        .withColumn("pipeline_run_id", expr("uuid()"))
        .withColumn("source_file", input_file_name())
        .withColumn(
            "file_name",
            regexp_extract(input_file_name(), "([^/]+$)", 1)
        )
    )

    return df