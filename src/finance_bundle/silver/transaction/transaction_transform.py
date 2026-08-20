"""
Transaction Silver transformations.

Responsibilities:
    - Standardize transaction_type
    - Standardize status
    - Standardize channel
    - Cast numeric fields
    - Cast transaction_timestamp
    - Preserve source columns
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ==========================================================
# Normalize Transactions
# ==========================================================

def normalize_transactions(
    df: DataFrame,
) -> DataFrame:

    return (
        df

        # --------------------------------------------------
        # Transaction ID
        # --------------------------------------------------

        .withColumn(
            "transaction_id",
            F.trim(
                F.col("transaction_id")
            ),
        )

        # --------------------------------------------------
        # Account ID
        # --------------------------------------------------

        .withColumn(
            "account_id",
            F.trim(
                F.col("account_id")
            ),
        )

        # --------------------------------------------------
        # Customer ID
        # --------------------------------------------------

        .withColumn(
            "customer_id",
            F.trim(
                F.col("customer_id")
            ),
        )

        # --------------------------------------------------
        # Transaction Type
        #
        # Source examples:
        # ATM
        # Transfer
        # Deposit
        # UPI
        # Withdrawal
        #
        # Convert to a consistent uppercase representation.
        # --------------------------------------------------

        .withColumn(
            "transaction_type",
            F.upper(
                F.trim(
                    F.col("transaction_type")
                )
            ),
        )

        # --------------------------------------------------
        # Amount
        # --------------------------------------------------

        .withColumn(
            "amount",
            F.col("amount").cast("decimal(18,2)"),
        )

        # --------------------------------------------------
        # Balance After Transaction
        # --------------------------------------------------

        .withColumn(
            "balance_after_transaction",
            F.col(
                "balance_after_transaction"
            ).cast("decimal(18,2)"),
        )

        # --------------------------------------------------
        # Channel
        # --------------------------------------------------

        .withColumn(
            "channel",
            F.upper(
                F.trim(
                    F.col("channel")
                )
            ),
        )

        # --------------------------------------------------
        # Status
        #
        # IMPORTANT:
        # The source column is "status", NOT
        # "transaction_status".
        # --------------------------------------------------

        .withColumn(
            "status",
            F.upper(
                F.trim(
                    F.col("status")
                )
            ),
        )

        # --------------------------------------------------
        # Fraud Flag
        # --------------------------------------------------

        .withColumn(
            "fraud_flag",
            F.col("fraud_flag").cast("boolean"),
        )

        # --------------------------------------------------
        # Transaction Timestamp
        # --------------------------------------------------

        .withColumn(
            "transaction_timestamp",
            F.to_timestamp(
                F.col("transaction_timestamp")
            ),
        )
    )