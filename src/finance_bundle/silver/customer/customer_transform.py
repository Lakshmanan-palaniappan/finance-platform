"""
Customer Silver transformations.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_customer(
    df: DataFrame,
) -> DataFrame:

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

    string_columns = [
        "customer_id",
        "branch_id",
        "first_name",
        "last_name",
        "gender",
        "mobile_number",
        "email",
        "pan_number",
        "aadhaar_number",
        "occupation",
        "city",
        "state",
        "customer_status",
    ]

    for column_name in string_columns:

        if column_name in df.columns:

            df = df.withColumn(
                column_name,
                F.trim(
                    F.col(column_name).cast("string")
                ),
            )

    if "email" in df.columns:

        df = df.withColumn(
            "email",
            F.lower(F.col("email")),
        )

    if "gender" in df.columns:

        df = df.withColumn(
            "gender",
            F.initcap(F.col("gender")),
        )

    if "customer_status" in df.columns:

        df = df.withColumn(
            "customer_status",
            F.upper(F.col("customer_status")),
        )

    if "dob" in df.columns:

        df = df.withColumn(
            "dob",
            F.to_date(F.col("dob")),
        )

    if "annual_income" in df.columns:

        df = df.withColumn(
            "annual_income",
            F.col("annual_income").cast("double"),
        )

    return df