from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ==========================================================
# CARD MASTER TRANSFORMATION
# ==========================================================

def transform_card_master(df: DataFrame) -> DataFrame:
    """
    Clean and standardize Bronze Card master data.
    """

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

    for column_name in string_columns:
        if column_name in df.columns:
            df = df.withColumn(
                column_name,
                F.trim(F.col(column_name))
            )

    # Normalize empty / NULL-like strings
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
                    None
                ).otherwise(F.col(column_name))
            )

    # Standardize categorical values
    df = (
        df
        .withColumn("card_type", F.upper(F.col("card_type")))
        .withColumn("network", F.upper(F.col("network")))
        .withColumn("status", F.upper(F.col("status")))
    )

    # Cast numeric columns
    df = (
        df
        .withColumn(
            "credit_limit",
            F.col("credit_limit").cast("double")
        )
        .withColumn(
            "daily_limit",
            F.col("daily_limit").cast("double")
        )
    )

    # Cast dates
    df = (
        df
        .withColumn(
            "issue_date",
            F.to_date(F.col("issue_date"))
        )
        .withColumn(
            "expiry_date",
            F.to_date(F.col("expiry_date"))
        )
    )

    # Remove duplicate Card master records
    df = df.dropDuplicates(["card_id"])

    return df


# ==========================================================
# CARD CDC TRANSFORMATION
# ==========================================================

def transform_card_cdc(df: DataFrame) -> DataFrame:
    """
    Clean and standardize Bronze Card CDC events.
    """

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

    for column_name in string_columns:
        if column_name in df.columns:
            df = df.withColumn(
                column_name,
                F.trim(F.col(column_name))
            )

    # Normalize CDC values
    df = (
        df
        .withColumn("entity", F.lower(F.col("entity")))
        .withColumn("operation", F.lower(F.col("operation")))
        .withColumn("card_type", F.upper(F.col("card_type")))
        .withColumn("network", F.upper(F.col("network")))
        .withColumn("old_status", F.upper(F.col("old_status")))
        .withColumn("new_status", F.upper(F.col("new_status")))
    )

    # Convert timestamps
    df = (
        df
        .withColumn(
            "event_timestamp",
            F.to_timestamp(F.col("event_timestamp"))
        )
        .withColumn(
            "change_timestamp",
            F.to_timestamp(F.col("change_timestamp"))
        )
    )

    # Remove duplicate CDC events
    df = df.dropDuplicates(["event_id"])

    return df


# ==========================================================
# BUILD COMPLETE CDC AFTER IMAGE
# ==========================================================

def build_card_after_image(
    card_master: DataFrame,
    card_cdc: DataFrame
) -> DataFrame:
    """
    Build a complete Card record from the current Card master
    and CDC event.

    CDC contains partial changed information, so the existing
    Card master is used as the base record.
    """

    master = card_master.alias("m")
    cdc = card_cdc.alias("c")

    joined = cdc.join(
        master,
        F.col("c.card_id") == F.col("m.card_id"),
        "left"
    )

    result = joined.select(
        F.col("c.card_id").alias("card_id"),

        F.coalesce(
            F.col("c.account_id"),
            F.col("m.account_id")
        ).alias("account_id"),

        F.coalesce(
            F.col("c.customer_id"),
            F.col("m.customer_id")
        ).alias("customer_id"),

        F.col("m.card_number").alias("card_number"),

        F.coalesce(
            F.col("c.card_type"),
            F.col("m.card_type")
        ).alias("card_type"),

        F.coalesce(
            F.col("c.network"),
            F.col("m.network")
        ).alias("network"),

        F.col("m.credit_limit").alias("credit_limit"),

        F.col("m.daily_limit").alias("daily_limit"),

        F.col("m.cvv").alias("cvv"),

        F.col("m.issue_date").alias("issue_date"),

        F.col("m.expiry_date").alias("expiry_date"),

        F.coalesce(
            F.col("c.new_status"),
            F.col("m.status")
        ).alias("status"),

        F.col("c.operation").alias("operation"),
        F.col("c.event_id").alias("event_id"),
        F.col("c.batch_id").alias("batch_id"),
        F.col("c.source_system").alias("source_system"),
        F.col("c.event_timestamp").alias("event_timestamp"),
        F.col("c.change_timestamp").alias("change_timestamp"),
    )

    return result