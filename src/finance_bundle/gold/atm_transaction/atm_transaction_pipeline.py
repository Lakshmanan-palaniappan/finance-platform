from pyspark import pipelines as dp

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables

from finance_bundle.gold.atm_transaction.atm_transaction_flow import (
    transform_gold_atm_summary,
)


# ==========================================================
# GOLD ATM SUMMARY PIPELINE
# ==========================================================

@dp.materialized_view(
    name=Catalog.gold(Tables.ATM_SUMMARY),
    comment="""
    Gold ATM transaction summary containing daily ATM transaction
    volume, customer/account/card counts, withdrawal metrics,
    and transaction status KPIs.
    """,
)
def atm_summary():

    # ------------------------------------------------------
    # Read Silver ATM Transaction
    # ------------------------------------------------------

    atm_df = dp.read(
        Catalog.silver(Tables.ATM_TRANSACTION)
    )

    # ------------------------------------------------------
    # Transform to Gold
    # ------------------------------------------------------

    return transform_gold_atm_summary(atm_df)