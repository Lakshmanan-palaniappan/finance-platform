from pyspark.sql import functions as F

from finance_bundle.bronze.transaction.transaction_streaming import (
    read_transaction_stream,
)


# ==========================================================
# Transaction Bronze Ingestion
# ==========================================================

def read_transaction_data():

    df = read_transaction_stream()

    return (
        df

        # --------------------------------------------------
        # Ingestion timestamp
        # --------------------------------------------------

        .withColumn(
            "ingestion_timestamp",
            F.current_timestamp(),
        )

        # --------------------------------------------------
        # Ingestion date
        # --------------------------------------------------

        .withColumn(
            "ingestion_date",
            F.current_date(),
        )

        # --------------------------------------------------
        # Pipeline run ID
        # --------------------------------------------------

        .withColumn(
            "pipeline_run_id",
            F.expr("uuid()"),
        )

        # --------------------------------------------------
        # Source file
        # --------------------------------------------------

        .withColumn(
            "source_file",
            F.col(
                "_metadata.file_path"
            ),
        )

        # --------------------------------------------------
        # File name
        # --------------------------------------------------

        .withColumn(
            "file_name",
            F.regexp_extract(
                F.col(
                    "_metadata.file_path"
                ),
                "([^/]+$)",
                1,
            ),
        )

        # --------------------------------------------------
        # File size
        # --------------------------------------------------

        .withColumn(
            "file_size",
            F.col(
                "_metadata.file_size"
            ),
        )

        # --------------------------------------------------
        # File modification time
        # --------------------------------------------------

        .withColumn(
            "file_modification_time",
            F.col(
                "_metadata.file_modification_time"
            ),
        )
    )