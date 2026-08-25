from pyspark import pipelines as dp

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables

from finance_bundle.gold.atm_transaction.atm_transaction_flow import (
    transform_gold_atm_summary,
)


# ==========================================================
# Gold ATM Summary
# ==========================================================

@dp.materialized_view(
    name=Catalog.gold(
        Tables.ATM_SUMMARY
    ),
    comment="""
    Gold ATM Summary containing daily ATM transaction volume,
    withdrawal metrics, transaction status KPIs, customer/account/card
    counts, and branch-level ATM performance.
    """,
)
def atm_summary():

    # ------------------------------------------------------
    # Read Silver ATM Transaction
    # ------------------------------------------------------

    atm_df = dp.read(
        Catalog.silver(
            Tables.ATM_TRANSACTION
        )
    )

    # ------------------------------------------------------
    # Read Silver Customer
    # ------------------------------------------------------

    customer_df = dp.read(
        Catalog.silver(
            Tables.CUSTOMER
        )
    )

    # ------------------------------------------------------
    # Read Silver Account
    # ------------------------------------------------------

    account_df = dp.read(
        Catalog.silver(
            Tables.ACCOUNT
        )
    )

    # ------------------------------------------------------
    # Read Silver Card
    # ------------------------------------------------------

    card_df = dp.read(
        Catalog.silver(
            Tables.CARD
        )
    )

    # ------------------------------------------------------
    # Read Silver Branch
    # ------------------------------------------------------

    branch_df = dp.read(
        Catalog.silver(
            Tables.BRANCH
        )
    )

    # ------------------------------------------------------
    # Build Gold ATM Summary
    # ------------------------------------------------------

    return transform_gold_atm_summary(
        atm_df=atm_df,
        customer_df=customer_df,
        account_df=account_df,
        card_df=card_df,
        branch_df=branch_df,
    )