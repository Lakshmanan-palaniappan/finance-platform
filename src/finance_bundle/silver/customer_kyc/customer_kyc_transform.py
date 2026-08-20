"""
Customer KYC Silver transformations.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ==========================================================
# Normalize Customer KYC
# ==========================================================

def normalize_customer_kyc(
    df: DataFrame,
) -> DataFrame:

    # ======================================================
    # Normalize Raw String Columns
    # ======================================================

    string_columns = [
        "kyc_id",
        "customer_id",
        "document_type",
        "pan_number",
        "aadhaar_number",
        "verified_by",
        "status",
    ]

    for column_name in string_columns:

        if column_name in df.columns:

            df = df.withColumn(
                column_name,
                F.trim(
                    F.col(column_name).cast("string")
                ),
            )

    # ======================================================
    # Convert Empty Strings to NULL
    # ======================================================

    for column_name in string_columns:

        if column_name in df.columns:

            df = df.withColumn(
                column_name,
                F.when(
                    F.length(
                        F.trim(
                            F.col(column_name)
                        )
                    ) == 0,
                    F.lit(None),
                ).otherwise(
                    F.col(column_name)
                ),
            )

    # ======================================================
    # Standardize Document Type
    # ======================================================

    df = df.withColumn(
        "document_type",
        F.upper(
            F.trim(
                F.col("document_type")
            )
        ),
    )

    # ======================================================
    # Standardize Status
    # ======================================================

    df = df.withColumn(
        "status",
        F.upper(
            F.trim(
                F.col("status")
            )
        ),
    )

    # ======================================================
    # Standardize PAN
    # ======================================================

    df = df.withColumn(
        "pan_number",
        F.upper(
            F.trim(
                F.col("pan_number")
            )
        ),
    )

    # ======================================================
    # Normalize Aadhaar
    # ======================================================

    df = df.withColumn(
        "aadhaar_number",
        F.regexp_replace(
            F.col("aadhaar_number"),
            r"\s+",
            "",
        ),
    )

    # ======================================================
    # PAN Verification
    # ======================================================
    #
    # PAN is verified when:
    #
    # document_type = PAN
    # status = VERIFIED
    #
    # ======================================================

    df = df.withColumn(
        "pan_verified",
        (
            (F.col("document_type") == "PAN")
            &
            (F.col("status") == "VERIFIED")
        ),
    )

    # ======================================================
    # Aadhaar Verification
    # ======================================================
    #
    # Aadhaar is verified when:
    #
    # document_type = AADHAAR
    # status = VERIFIED
    #
    # ======================================================

    df = df.withColumn(
        "aadhaar_verified",
        (
            (F.col("document_type") == "AADHAAR")
            &
            (F.col("status") == "VERIFIED")
        ),
    )

    # ======================================================
    # KYC Status
    # ======================================================

    df = df.withColumn(
        "kyc_status",
        F.when(
            F.col("status") == "VERIFIED",
            F.lit("VERIFIED"),
        )
        .when(
            F.col("status") == "PENDING",
            F.lit("PENDING"),
        )
        .when(
            F.col("status") == "REJECTED",
            F.lit("REJECTED"),
        )
        .otherwise(
            F.lit("UNKNOWN")
        ),
    )

    # ======================================================
    # Expiry Date
    # ======================================================
    #
    # The current source does not contain expiry_date.
    # Do not invent an expiry date.
    #
    # Keep the column in Silver as NULL so the business
    # schema remains stable if expiry_date is introduced
    # by the source later.
    #
    # ======================================================

    df = df.withColumn(
        "expiry_date",
        F.lit(None).cast("date"),
    )

    # ======================================================
    # Remove Duplicate KYC Records
    #
    # kyc_id is the natural record identifier.
    # ======================================================

    df = df.dropDuplicates(
        ["kyc_id"]
    )

    # ======================================================
    # Final Silver Projection
    # ======================================================

    metadata_columns = [
        "ingestion_timestamp",
        "ingestion_date",
        "pipeline_run_id",
        "source_file",
        "file_name",
        "file_size",
        "file_modification_time",
        "_rescued_data",
    ]

    available_metadata = [
        column_name
        for column_name in metadata_columns
        if column_name in df.columns
    ]

    return df.select(
        "customer_id",
        "pan_verified",
        "aadhaar_verified",
        "kyc_status",
        "expiry_date",

        # ----------------------------------------------
        # Source attributes retained in Silver
        # ----------------------------------------------

        "kyc_id",
        "document_type",
        "pan_number",
        "aadhaar_number",
        "verification_date",
        "verified_by",
        "status",

        # ----------------------------------------------
        # Metadata
        # ----------------------------------------------

        *available_metadata,
    )