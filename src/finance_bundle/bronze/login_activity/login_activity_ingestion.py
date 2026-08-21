from pyspark.sql import functions as F

from finance_bundle.bronze.login_activity.login_activity_streaming import (
    read_login_activity_stream,
)


# ==========================================================
# Bronze Login Activity Ingestion
# ==========================================================

def read_login_activity_data():

    df = read_login_activity_stream()

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
        # Source file path
        # --------------------------------------------------

        .withColumn(
            "source_file",
            F.col(
                "_metadata.file_path"
            ),
        )

        # --------------------------------------------------
        # Source file name
        # --------------------------------------------------

        .withColumn(
            "file_name",
            F.regexp_extract(
                F.col("_metadata.file_path"),
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