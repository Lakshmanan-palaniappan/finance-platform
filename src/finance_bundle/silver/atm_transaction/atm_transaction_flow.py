from pyspark import pipelines as dp
from pyspark.sql.functions import col

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables

from finance_bundle.silver.atm_transaction.atm_transaction_transform import (
    clean_atm_transaction,
    validate_atm_transaction,
    add_validation_result,
    transform_silver_atm_transaction,
)


# ==========================================================
# Silver ATM Transaction
# ==========================================================

@dp.table(
    name=Catalog.silver(
        Tables.ATM_TRANSACTION
    ),
    comment="Silver ATM Transaction Table",
)
def atm_transaction_silver():

    df = dp.read_stream(
        Catalog.bronze(
            Tables.ATM_TRANSACTION
        )
    )

    return transform_silver_atm_transaction(df)


# ==========================================================
# ATM Transaction Quarantine
# ==========================================================

@dp.table(
    name=Catalog.quarantine(
        Tables.ATM_TRANSACTION_QUARANTINE
    ),
    comment="Quarantined invalid ATM Transaction records",
)
def atm_transaction_quarantine():

    df = dp.read_stream(
        Catalog.bronze(
            Tables.ATM_TRANSACTION
        )
    )

    df = clean_atm_transaction(df)

    df = validate_atm_transaction(df)

    df = add_validation_result(df)

    return (
        df
        .filter(
            col("_is_valid") == False
        )
    )