"""
Account Silver transformations.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_account(
    df: DataFrame,
) -> DataFrame:

    # ======================================================
    # Normalize Column Names
    # ======================================================

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

    # ======================================================
    # String Columns
    # ======================================================

    string_columns = [
        "account_id",
        "account_number",
        "customer_id",
        "branch_id",
        "account_type",
        "account_status",
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
    # Account Type
    # ======================================================

    if "account_type" in df.columns:

        df = df.withColumn(
            "account_type",
            F.initcap(
                F.lower(
                    F.col("account_type")
                )
            ),
        )

    # ======================================================
    # Account Status
    # ======================================================

    if "account_status" in df.columns:

        df = df.withColumn(
            "account_status",
            F.upper(
                F.col("account_status")
            ),
        )

    # ======================================================
    # Balance
    # ======================================================

    if "balance" in df.columns:

        df = df.withColumn(
            "balance",
            F.col("balance").cast(
                "decimal(18,2)"
            ),
        )

    # ======================================================
    # Minimum Balance
    # ======================================================

    if "minimum_balance" in df.columns:

        df = df.withColumn(
            "minimum_balance",
            F.col("minimum_balance").cast(
                "decimal(18,2)"
            ),
        )

    # ======================================================
    # Interest Rate
    # ======================================================

    if "interest_rate" in df.columns:

        df = df.withColumn(
            "interest_rate",
            F.col("interest_rate").cast(
                "decimal(5,2)"
            ),
        )

    # ======================================================
    # Opened Date
    # ======================================================

    if "opened_date" in df.columns:

        df = df.withColumn(
            "opened_date",
            F.to_date(
                F.col("opened_date")
            ),
        )

    return df