from pyspark import pipelines as dp
from pyspark.sql import functions as F

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables

from finance_bundle.silver.transaction.transaction_flow import (
    apply_transaction_streaming_rules,
)

from finance_bundle.silver.transaction.transaction_transform import (
    normalize_transactions,
)


# ==========================================================
# Tables
# ==========================================================

BRONZE_TRANSACTION = Catalog.bronze(
    Tables.TRANSACTION
)

SILVER_TRANSACTION = Catalog.silver(
    Tables.TRANSACTION
)

QUARANTINE_TRANSACTION = Catalog.quarantine(
    Tables.TRANSACTION_QUARANTINE
)


# ==========================================================
# Valid Transaction Conditions
# ==========================================================

VALID_TRANSACTION_TYPES = (
    "DEPOSIT",
    "WITHDRAWAL",
    "TRANSFER",
    "UPI",
    "ATM",
)

VALID_STATUSES = (
    "SUCCESS",
    "FAILED",
)


# ==========================================================
# Silver Transaction
# ==========================================================

@dp.table(
    name=SILVER_TRANSACTION,
    comment=(
        "Silver transaction streaming table "
        "with standardized fields, validation, "
        "watermarking and deduplication"
    ),
)
@dp.expect(
    "transaction_id_not_null",
    "transaction_id IS NOT NULL",
)
@dp.expect(
    "account_id_not_null",
    "account_id IS NOT NULL",
)
@dp.expect(
    "customer_id_not_null",
    "customer_id IS NOT NULL",
)
@dp.expect(
    "transaction_timestamp_not_null",
    "transaction_timestamp IS NOT NULL",
)
@dp.expect(
    "amount_positive",
    "amount > 0",
)
@dp.expect(
    "transaction_type_valid",
    """
    transaction_type IN (
        'DEPOSIT',
        'WITHDRAWAL',
        'TRANSFER',
        'UPI',
        'ATM'
    )
    """,
)
@dp.expect(
    "status_valid",
    """
    status IN (
        'SUCCESS',
        'FAILED'
    )
    """,
)
def transaction_silver():

    # ------------------------------------------------------
    # Bronze → Silver streaming dependency
    # ------------------------------------------------------

    df = dp.read_stream(
        BRONZE_TRANSACTION
    )

    # ------------------------------------------------------
    # Standardize source data
    # ------------------------------------------------------

    df = normalize_transactions(
        df
    )

    # ------------------------------------------------------
    # Watermark + deduplication
    # ------------------------------------------------------

    df = apply_transaction_streaming_rules(
        df
    )

    # ------------------------------------------------------
    # Silver should contain only valid records
    # ------------------------------------------------------

    return df.filter(
        F.col("transaction_id").isNotNull()
        &
        F.col("account_id").isNotNull()
        &
        F.col("customer_id").isNotNull()
        &
        F.col(
            "transaction_timestamp"
        ).isNotNull()
        &
        F.col("amount").isNotNull()
        &
        (
            F.col("amount") > 0
        )
        &
        F.col(
            "transaction_type"
        ).isin(
            *VALID_TRANSACTION_TYPES
        )
        &
        F.col(
            "status"
        ).isin(
            *VALID_STATUSES
        )
    )


# ==========================================================
# Transaction Quarantine
# ==========================================================

@dp.table(
    name=QUARANTINE_TRANSACTION,
    comment=(
        "Transaction records that fail "
        "Silver validation"
    ),
)
def transaction_quarantine():

    # ------------------------------------------------------
    # Read Bronze stream
    # ------------------------------------------------------

    df = dp.read_stream(
        BRONZE_TRANSACTION
    )

    # ------------------------------------------------------
    # Standardize source data
    # ------------------------------------------------------

    df = normalize_transactions(
        df
    )

    # ------------------------------------------------------
    # Apply same streaming rules
    # ------------------------------------------------------

    df = apply_transaction_streaming_rules(
        df
    )

    # ------------------------------------------------------
    # Keep invalid records
    # ------------------------------------------------------

    return df.filter(
        F.col("transaction_id").isNull()
        |
        F.col("account_id").isNull()
        |
        F.col("customer_id").isNull()
        |
        F.col(
            "transaction_timestamp"
        ).isNull()
        |
        F.col("amount").isNull()
        |
        (
            F.col("amount") <= 0
        )
        |
        ~F.col(
            "transaction_type"
        ).isin(
            *VALID_TRANSACTION_TYPES
        )
        |
        ~F.col(
            "status"
        ).isin(
            *VALID_STATUSES
        )
    )