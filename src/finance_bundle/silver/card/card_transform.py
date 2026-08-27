from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ==========================================================
# CARD MASTER TRANSFORMATION
# ==========================================================

def transform_card_master(
    df: DataFrame
) -> DataFrame:

    string_columns = [
        "card_id",
        "account_id",
        "customer_id",
        "card_number",
        "card_type",
        "network",
        "cvv",
        "status",
    ]

    # ------------------------------------------------------
    # Trim strings
    # ------------------------------------------------------

    for column_name in string_columns:

        if column_name in df.columns:

            df = df.withColumn(
                column_name,
                F.trim(
                    F.col(column_name).cast("string")
                )
            )

    # ------------------------------------------------------
    # Normalize NULL-like values
    # ------------------------------------------------------

    for column_name in string_columns:

        if column_name in df.columns:

            df = df.withColumn(
                column_name,
                F.when(
                    F.col(column_name).isin(
                        "",
                        "NULL",
                        "null",
                        "N/A",
                        "NA"
                    ),
                    F.lit(None)
                ).otherwise(
                    F.col(column_name)
                )
            )

    # ------------------------------------------------------
    # Standardize categorical fields
    # ------------------------------------------------------

    if "card_type" in df.columns:

        df = df.withColumn(
            "card_type",
            F.upper(
                F.col("card_type")
            )
        )

    if "network" in df.columns:

        df = df.withColumn(
            "network",
            F.upper(
                F.col("network")
            )
        )

    if "status" in df.columns:

        df = df.withColumn(
            "status",
            F.upper(
                F.col("status")
            )
        )

    # ------------------------------------------------------
    # Numeric fields
    # ------------------------------------------------------

    if "credit_limit" in df.columns:

        df = df.withColumn(
            "credit_limit",
            F.col("credit_limit").cast("double")
        )

    if "daily_limit" in df.columns:

        df = df.withColumn(
            "daily_limit",
            F.col("daily_limit").cast("double")
        )

    # ------------------------------------------------------
    # Date fields
    # ------------------------------------------------------

    if "issue_date" in df.columns:

        df = df.withColumn(
            "issue_date",
            F.to_date(
                F.col("issue_date")
            )
        )

    if "expiry_date" in df.columns:

        df = df.withColumn(
            "expiry_date",
            F.to_date(
                F.col("expiry_date")
            )
        )

    # ------------------------------------------------------
    # Remove duplicate cards
    # ------------------------------------------------------

    return df.dropDuplicates(
        ["card_id"]
    )


# ==========================================================
# CARD CDC TRANSFORMATION
# ==========================================================

def transform_card_cdc(
    df: DataFrame
) -> DataFrame:

    string_columns = [
        "entity",
        "operation",
        "card_id",
        "customer_id",
        "account_id",
        "old_status",
        "new_status",
        "card_type",
        "network",
        "event_id",
        "batch_id",
        "source_system",
    ]

    # ------------------------------------------------------
    # Trim strings
    # ------------------------------------------------------

    for column_name in string_columns:

        if column_name in df.columns:

            df = df.withColumn(
                column_name,
                F.trim(
                    F.col(column_name).cast("string")
                )
            )

    # ------------------------------------------------------
    # Standardize values
    # ------------------------------------------------------

    if "entity" in df.columns:

        df = df.withColumn(
            "entity",
            F.lower(
                F.col("entity")
            )
        )

    if "operation" in df.columns:

        df = df.withColumn(
            "operation",
            F.lower(
                F.col("operation")
            )
        )

    if "card_type" in df.columns:

        df = df.withColumn(
            "card_type",
            F.upper(
                F.col("card_type")
            )
        )

    if "network" in df.columns:

        df = df.withColumn(
            "network",
            F.upper(
                F.col("network")
            )
        )

    if "old_status" in df.columns:

        df = df.withColumn(
            "old_status",
            F.upper(
                F.col("old_status")
            )
        )

    if "new_status" in df.columns:

        df = df.withColumn(
            "new_status",
            F.upper(
                F.col("new_status")
            )
        )

    # ------------------------------------------------------
    # Timestamp conversion
    # ------------------------------------------------------

    if "event_timestamp" in df.columns:

        df = df.withColumn(
            "event_timestamp",
            F.to_timestamp(
                F.col("event_timestamp")
            )
        )

    if "change_timestamp" in df.columns:

        df = df.withColumn(
            "change_timestamp",
            F.to_timestamp(
                F.col("change_timestamp")
            )
        )

    # ------------------------------------------------------
    # Remove duplicate CDC events
    # ------------------------------------------------------

    if "event_id" in df.columns:

        df = df.dropDuplicates(
            ["event_id"]
        )

    return df