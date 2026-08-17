from pyspark.sql.functions import (
    col,
    current_timestamp,
    current_date,
    regexp_extract,
    expr,
)

from finance_bundle.bronze.atm_transaction.atm_transaction_streaming import (
    read_atm_transaction_stream,
)


def read_atm_transaction_data():

    df = read_atm_transaction_stream()

    # ==================================================
    # Bronze ingestion metadata
    # ==================================================

    df = (
        df

        # ----------------------------------------------
        # Ingestion timestamp
        # ----------------------------------------------

        .withColumn(
            "ingestion_timestamp",
            current_timestamp(),
        )

        # ----------------------------------------------
        # Ingestion date
        # ----------------------------------------------

        .withColumn(
            "ingestion_date",
            current_date(),
        )

        # ----------------------------------------------
        # Pipeline run ID
        # ----------------------------------------------

        .withColumn(
            "pipeline_run_id",
            expr("uuid()"),
        )

        # ----------------------------------------------
        # Complete source file path
        # ----------------------------------------------

        .withColumn(
            "source_file",
            col("_metadata.file_path"),
        )

        # ----------------------------------------------
        # Source file name
        # ----------------------------------------------

        .withColumn(
            "file_name",
            regexp_extract(
                col("_metadata.file_path"),
                "([^/]+$)",
                1,
            ),
        )

        # ----------------------------------------------
        # File size
        # ----------------------------------------------

        .withColumn(
            "file_size",
            col("_metadata.file_size"),
        )

        # ----------------------------------------------
        # File modification time
        # ----------------------------------------------

        .withColumn(
            "file_modification_time",
            col("_metadata.file_modification_time"),
        )
    )

    return df