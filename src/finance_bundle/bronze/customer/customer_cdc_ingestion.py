from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    current_date,
    current_timestamp,
    regexp_extract,
    trim,
    lower,
)

from finance_bundle.common.config import settings

from finance_bundle.common.paths import (
    CDC,
    SchemaLocation,
)

from finance_bundle.schemas.customer_cdc_schema import (
    CUSTOMER_CDC_SCHEMA,
)


def read_customer_cdc_data():

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
            SchemaLocation.CUSTOMER_CDC,
        )

        .option(
            "cloudFiles.schemaEvolutionMode",
            "addNewColumns",
        )

        .option(
            "rescuedDataColumn",
            "_rescued_data",
        )

        .load(CDC.CUSTOMER)
    )

    # ----------------------------------------------------------
    # Normalize CDC column names
    # ----------------------------------------------------------

    for column_name in df.columns:

        normalized = (
            column_name
            .strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
        )

        if normalized != column_name:
            df = df.withColumnRenamed(
                column_name,
                normalized,
            )

    # ----------------------------------------------------------
    # Normalize values
    # ----------------------------------------------------------

    df = (
        df
        .withColumn(
            "entity",
            lower(trim(col("entity"))),
        )
        .withColumn(
            "operation",
            lower(trim(col("operation"))),
        )
        .withColumn(
            "customer_id",
            trim(col("customer_id")),
        )
        .withColumn(
            "attribute",
            lower(trim(col("attribute"))),
        )
    )

    # ----------------------------------------------------------
    # Metadata
    # ----------------------------------------------------------

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