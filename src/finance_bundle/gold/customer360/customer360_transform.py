"""
Customer 360 Gold transformations.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_customer360(
    customer_df: DataFrame,
) -> DataFrame:

    # ======================================================
    # Derive SCD2 business metadata
    # ======================================================

    df = (
        customer_df

        .withColumn(
            "effective_start",
            F.col("__START_AT"),
        )

        .withColumn(
            "effective_end",
            F.col("__END_AT"),
        )

        .withColumn(
            "is_current",
            F.col("__END_AT").isNull(),
        )
    )

    # ======================================================
    # Customer name
    # ======================================================

    df = df.withColumn(
        "customer_name",
        F.trim(
            F.concat_ws(
                " ",
                F.col("first_name"),
                F.col("last_name"),
            )
        ),
    )

    # ======================================================
    # Age
    #
    # Calculate age as of current date.
    # ======================================================

    df = df.withColumn(
        "age",
        F.when(
            F.col("dob").isNotNull(),
            F.floor(
                F.months_between(
                    F.current_date(),
                    F.col("dob"),
                ) / 12
            ).cast("integer"),
        ),
    )

    # ======================================================
    # Income category
    # ======================================================

    df = df.withColumn(
        "income_category",

        F.when(
            F.col("annual_income").isNull(),
            F.lit("UNKNOWN"),
        )

        .when(
            F.col("annual_income") < 500000,
            F.lit("LOW"),
        )

        .when(
            F.col("annual_income") < 1500000,
            F.lit("MEDIUM"),
        )

        .when(
            F.col("annual_income") < 3000000,
            F.lit("HIGH"),
        )

        .otherwise(
            F.lit("VERY_HIGH"),
        ),
    )

    # ======================================================
    # Active customer
    #
    # A customer is current AND has ACTIVE status.
    # ======================================================

    df = df.withColumn(
        "customer_active_flag",

        (
            F.col("is_current")
            &
            (
                F.upper(
                    F.trim(
                        F.col("customer_status")
                    )
                )
                == F.lit("ACTIVE")
            )
        ),
    )

    # ======================================================
    # Final Gold projection
    # ======================================================

    return df.select(

        # --------------------------------------------------
        # Customer identity
        # --------------------------------------------------

        "customer_id",

        "branch_id",

        "customer_name",

        "first_name",

        "last_name",

        "gender",

        "dob",

        "age",

        # --------------------------------------------------
        # Contact
        # --------------------------------------------------

        "mobile_number",

        "email",

        # --------------------------------------------------
        # KYC
        # --------------------------------------------------

        "pan_number",

        "aadhaar_number",

        # --------------------------------------------------
        # Customer profile
        # --------------------------------------------------

        "occupation",

        "annual_income",

        "income_category",

        "city",

        "state",

        "customer_status",

        "customer_active_flag",

        # --------------------------------------------------
        # SCD2 history
        # --------------------------------------------------

        "effective_start",

        "effective_end",

        "is_current",
    )