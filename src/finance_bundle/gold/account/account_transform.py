"""
Account Gold transformations.

Creates:

1. Account Portfolio Summary
2. Account Balance Summary
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ==========================================================
# Current Account Records
# ==========================================================

def current_accounts(
    account_df: DataFrame,
) -> DataFrame:

    return (
        account_df
        .filter(
            F.col("__END_AT").isNull()
        )
    )


# ==========================================================
# Portfolio Summary
# ==========================================================

def build_portfolio_summary(
    account_df: DataFrame,
) -> DataFrame:

    df = current_accounts(
        account_df
    )

    return (
        df
        .groupBy(
            "branch_id",
            "account_type",
        )
        .agg(

            # ----------------------------------------------
            # Account Counts
            # ----------------------------------------------

            F.count(
                "account_id"
            ).alias(
                "total_accounts"
            ),

            F.sum(
                F.when(
                    F.col("account_status")
                    == "ACTIVE",
                    1,
                ).otherwise(0)
            ).alias(
                "active_accounts"
            ),

            F.sum(
                F.when(
                    F.col("account_status")
                    == "DORMANT",
                    1,
                ).otherwise(0)
            ).alias(
                "dormant_accounts"
            ),

            F.sum(
                F.when(
                    F.col("account_status")
                    == "CLOSED",
                    1,
                ).otherwise(0)
            ).alias(
                "closed_accounts"
            ),

            # ----------------------------------------------
            # Balance Metrics
            # ----------------------------------------------

            F.sum(
                "balance"
            ).alias(
                "total_balance"
            ),

            F.avg(
                "balance"
            ).alias(
                "average_balance"
            ),

            F.min(
                "balance"
            ).alias(
                "minimum_account_balance"
            ),

            F.max(
                "balance"
            ).alias(
                "maximum_account_balance"
            ),

        )
    )


# ==========================================================
# Balance Summary
# ==========================================================

def build_balance_summary(
    account_df: DataFrame,
) -> DataFrame:

    df = current_accounts(
        account_df
    )

    return (
        df
        .groupBy(
            "account_type",
        )
        .agg(

            # ----------------------------------------------
            # Account Count
            # ----------------------------------------------

            F.count(
                "account_id"
            ).alias(
                "account_count"
            ),

            # ----------------------------------------------
            # Balance
            # ----------------------------------------------

            F.sum(
                "balance"
            ).alias(
                "total_balance"
            ),

            F.avg(
                "balance"
            ).alias(
                "average_balance"
            ),

            F.min(
                "balance"
            ).alias(
                "minimum_balance"
            ),

            F.max(
                "balance"
            ).alias(
                "maximum_balance"
            ),

            # ----------------------------------------------
            # Required Minimum
            # ----------------------------------------------

            F.sum(
                "minimum_balance"
            ).alias(
                "total_required_minimum_balance"
            ),

            # ----------------------------------------------
            # Interest
            # ----------------------------------------------

            F.avg(
                "interest_rate"
            ).alias(
                "average_interest_rate"
            ),

        )
    )