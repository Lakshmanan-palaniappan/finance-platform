from pyspark.sql import DataFrame

from finance_bundle.gold.atm_transaction.atm_transaction_transform import (
    enrich_atm_transactions,
    build_atm_summary,
)


# ==========================================================
# Gold ATM Transaction Flow
# ==========================================================

def transform_gold_atm_summary(
    atm_df: DataFrame,
    customer_df: DataFrame,
    account_df: DataFrame,
    card_df: DataFrame,
    branch_df: DataFrame,
) -> DataFrame:

    # ------------------------------------------------------
    # Enrich ATM transactions
    # ------------------------------------------------------

    enriched_df = enrich_atm_transactions(
        atm_df=atm_df,
        customer_df=customer_df,
        account_df=account_df,
        card_df=card_df,
        branch_df=branch_df,
    )

    # ------------------------------------------------------
    # Build Gold ATM Summary
    # ------------------------------------------------------

    return build_atm_summary(
        enriched_df
    )