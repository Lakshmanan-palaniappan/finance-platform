from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    max,
    min,
    round,
    sum,
    to_date,
    when,
)


# ==========================================================
# BUILD GOLD ATM SUMMARY
# ==========================================================

def build_atm_summary(
    df: DataFrame,
) -> DataFrame:

    # ------------------------------------------------------
    # Create transaction date
    # ------------------------------------------------------

    df = df.withColumn(
        "transaction_date",
        to_date(col("transaction_timestamp")),
    )

    # ------------------------------------------------------
    # Aggregate ATM transactions
    # ------------------------------------------------------

    summary = (
        df
        .groupBy(
            "atm_id",
            "transaction_date",
        )
        .agg(

            # ------------------------------------------------
            # Transaction metrics
            # ------------------------------------------------

            count("*").alias(
                "total_transactions"
            ),

            countDistinct(
                "customer_id"
            ).alias(
                "unique_customers"
            ),

            countDistinct(
                "account_id"
            ).alias(
                "unique_accounts"
            ),

            countDistinct(
                "card_id"
            ).alias(
                "unique_cards"
            ),

            # ------------------------------------------------
            # Status metrics
            # ------------------------------------------------

            sum(
                when(
                    col("status") == "SUCCESS",
                    1,
                ).otherwise(0)
            ).alias(
                "successful_transactions"
            ),

            sum(
                when(
                    col("status") == "FAILED",
                    1,
                ).otherwise(0)
            ).alias(
                "failed_transactions"
            ),

            sum(
                when(
                    col("status") == "DECLINED",
                    1,
                ).otherwise(0)
            ).alias(
                "declined_transactions"
            ),

            # ------------------------------------------------
            # Withdrawal metrics
            # ------------------------------------------------

            round(
                sum("withdrawal_amount"),
                2,
            ).alias(
                "total_withdrawal_amount"
            ),

            round(
                avg("withdrawal_amount"),
                2,
            ).alias(
                "average_withdrawal_amount"
            ),

            round(
                min("withdrawal_amount"),
                2,
            ).alias(
                "minimum_withdrawal_amount"
            ),

            round(
                max("withdrawal_amount"),
                2,
            ).alias(
                "maximum_withdrawal_amount"
            ),
        )
    )

    # ======================================================
    # KPI calculations
    # ======================================================

    summary = (
        summary

        .withColumn(
            "success_rate",
            round(
                when(
                    col("total_transactions") > 0,
                    (
                        col("successful_transactions")
                        / col("total_transactions")
                    ) * 100,
                ).otherwise(0),
                2,
            ),
        )

        .withColumn(
            "failure_rate",
            round(
                when(
                    col("total_transactions") > 0,
                    (
                        col("failed_transactions")
                        / col("total_transactions")
                    ) * 100,
                ).otherwise(0),
                2,
            ),
        )

        .withColumn(
            "decline_rate",
            round(
                when(
                    col("total_transactions") > 0,
                    (
                        col("declined_transactions")
                        / col("total_transactions")
                    ) * 100,
                ).otherwise(0),
                2,
            ),
        )
    )

    # ======================================================
    # Business validations
    # ======================================================

    summary = (
        summary

        # Status counts must equal total transactions
        .withColumn(
            "status_count_valid",
            (
                col("successful_transactions")
                + col("failed_transactions")
                + col("declined_transactions")
            ) == col("total_transactions"),
        )

        # Withdrawal amount cannot be negative
        .withColumn(
            "amount_valid",
            col("total_withdrawal_amount") >= 0,
        )

        # Rates cannot exceed 100%
        .withColumn(
            "rate_valid",
            (
                col("success_rate")
                + col("failure_rate")
                + col("decline_rate")
            ) <= 100.01,
        )
    )

    # ======================================================
    # Final Gold output
    # ======================================================

    return (
        summary
        .filter(
            col("status_count_valid")
            & col("amount_valid")
            & col("rate_valid")
        )
        .drop(
            "status_count_valid",
            "amount_valid",
            "rate_valid",
        )
    )