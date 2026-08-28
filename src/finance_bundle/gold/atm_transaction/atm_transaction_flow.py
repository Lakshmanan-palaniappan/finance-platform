from pyspark.sql import DataFrame

from finance_bundle.gold.atm_transaction.atm_transaction_transform import (
    build_atm_summary,
)


# ==========================================================
# GOLD ATM TRANSACTION FLOW
# ==========================================================

def transform_gold_atm_summary(
    atm_df: DataFrame,
) -> DataFrame:

    return build_atm_summary(atm_df)