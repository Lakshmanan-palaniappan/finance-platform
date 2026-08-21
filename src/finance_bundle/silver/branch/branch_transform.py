"""
Branch Silver transformations.

Responsibilities:
- Normalize column names
- Trim whitespace
- Normalize null values
- Standardize string values
- Cast data types
- Remove duplicates
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ==========================================================
# Normalize Branch Data
# ==========================================================

def normalize_branch(
    df: DataFrame,
) -> DataFrame:

    # ------------------------------------------------------
    # Normalize column names
    # ------------------------------------------------------

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

    # ------------------------------------------------------
    # String columns
    # ------------------------------------------------------

    string_columns = [
        "branch_id",
        "branch_name",
        "branch_code",
        "ifsc_code",
        "bank_name",
        "city",
        "state",
        "zone",
        "country",
        "status",
    ]

    # ------------------------------------------------------
    # Trim whitespace and normalize empty strings to NULL
    # ------------------------------------------------------

    for column_name in string_columns:

        if column_name in df.columns:

            df = df.withColumn(
                column_name,
                F.when(
                    F.trim(
                        F.col(column_name).cast("string")
                    ) == "",
                    F.lit(None),
                ).otherwise(
                    F.trim(
                        F.col(column_name).cast("string")
                    )
                ),
            )

    # ------------------------------------------------------
    # Standardize branch code
    # ------------------------------------------------------

    if "branch_code" in df.columns:

        df = df.withColumn(
            "branch_code",
            F.upper(
                F.col("branch_code")
            ),
        )

    # ------------------------------------------------------
    # Standardize IFSC code
    # ------------------------------------------------------

    if "ifsc_code" in df.columns:

        df = df.withColumn(
            "ifsc_code",
            F.upper(
                F.col("ifsc_code")
            ),
        )

    # ------------------------------------------------------
    # Standardize bank name
    # ------------------------------------------------------

    if "bank_name" in df.columns:

        df = df.withColumn(
            "bank_name",
            F.initcap(
                F.col("bank_name")
            ),
        )

    # ------------------------------------------------------
    # Standardize city
    # ------------------------------------------------------

    if "city" in df.columns:

        df = df.withColumn(
            "city",
            F.initcap(
                F.col("city")
            ),
        )

    # ------------------------------------------------------
    # Standardize state
    # ------------------------------------------------------

    if "state" in df.columns:

        df = df.withColumn(
            "state",
            F.initcap(
                F.col("state")
            ),
        )

    # ------------------------------------------------------
    # Standardize zone
    # ------------------------------------------------------

    if "zone" in df.columns:

        df = df.withColumn(
            "zone",
            F.upper(
                F.col("zone")
            ),
        )

    # ------------------------------------------------------
    # Standardize country
    # ------------------------------------------------------

    if "country" in df.columns:

        df = df.withColumn(
            "country",
            F.initcap(
                F.col("country")
            ),
        )

    # ------------------------------------------------------
    # Standardize status
    # ------------------------------------------------------

    if "status" in df.columns:

        df = df.withColumn(
            "status",
            F.upper(
                F.col("status")
            ),
        )

    # ------------------------------------------------------
    # Remove duplicate Branch records
    # ------------------------------------------------------

    if "branch_id" in df.columns:

        df = df.dropDuplicates(
            ["branch_id"]
        )

    return df