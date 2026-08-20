from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ==========================================================
# Daily Transaction Summary
# ==========================================================

def build_daily_transaction_summary(
    df: DataFrame,
) -> DataFrame:

    return (
        df

        # --------------------------------------------------
        # Transaction Date
        # --------------------------------------------------

        .withColumn(
            "transaction_date",
            F.to_date(
                "transaction_timestamp"
            ),
        )

        # --------------------------------------------------
        # Daily aggregation
        # --------------------------------------------------

        .groupBy(
            "transaction_date",
            "transaction_type",
            "transaction_status",
            "channel",
            "currency",
        )

        .agg(

            # ----------------------------------------------
            # Number of transactions
            # ----------------------------------------------

            F.count(
                "transaction_id"
            ).alias(
                "transaction_count"
            ),

            # ----------------------------------------------
            # Total transaction amount
            # ----------------------------------------------

            F.sum(
                "amount"
            ).alias(
                "total_amount"
            ),

            # ----------------------------------------------
            # Average transaction amount
            # ----------------------------------------------

            F.round(
                F.avg("amount"),
                2,
            ).alias(
                "average_amount"
            ),

            # ----------------------------------------------
            # Successful transactions
            # ----------------------------------------------

            F.sum(
                F.when(
                    F.col(
                        "transaction_status"
                    ) == "SUCCESS",
                    1,
                ).otherwise(0)
            ).alias(
                "successful_transaction_count"
            ),

            # ----------------------------------------------
            # Failed transactions
            # ----------------------------------------------

            F.sum(
                F.when(
                    F.col(
                        "transaction_status"
                    ) == "FAILED",
                    1,
                ).otherwise(0)
            ).alias(
                "failed_transaction_count"
            ),
        )
    )