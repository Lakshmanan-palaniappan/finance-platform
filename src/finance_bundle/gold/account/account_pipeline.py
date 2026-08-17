"""
Account Gold Lakeflow SDP Pipeline.

Silver Account SCD2
       |
       +------------------------+
       |                        |
       v                        v
Portfolio Summary       Balance Summary
"""

from pyspark import pipelines as dp

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables

from finance_bundle.gold.account.account_transform import (
    build_portfolio_summary,
    build_balance_summary,
)


# ==========================================================
# Tables
# ==========================================================

SILVER_ACCOUNT = Catalog.silver(
    Tables.ACCOUNT
)

GOLD_ACCOUNT_PORTFOLIO = Catalog.gold(
    Tables.ACCOUNT_PORTFOLIO_SUMMARY
)

GOLD_ACCOUNT_BALANCE = Catalog.gold(
    Tables.ACCOUNT_BALANCE_SUMMARY
)


# ==========================================================
# Portfolio Summary
# ==========================================================

@dp.materialized_view(
    name=GOLD_ACCOUNT_PORTFOLIO,

    comment=(
        "Gold Account Portfolio Summary "
        "built from Silver Account SCD Type 2"
    ),
)
def account_portfolio_summary():

    account_df = dp.read(
        SILVER_ACCOUNT
    )

    return build_portfolio_summary(
        account_df
    )


# ==========================================================
# Balance Summary
# ==========================================================

@dp.materialized_view(
    name=GOLD_ACCOUNT_BALANCE,

    comment=(
        "Gold Account Balance Summary "
        "built from Silver Account SCD Type 2"
    ),
)
def account_balance_summary():

    account_df = dp.read(
        SILVER_ACCOUNT
    )

    return build_balance_summary(
        account_df
    )