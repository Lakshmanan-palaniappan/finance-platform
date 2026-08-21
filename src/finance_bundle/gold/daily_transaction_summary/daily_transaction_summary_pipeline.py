from pyspark import pipelines as dp

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables

from finance_bundle.gold.daily_transaction_summary.daily_transaction_summary_transform import (
    build_daily_transaction_summary,
)


# ==========================================================
# Source
# ==========================================================

SILVER_TRANSACTION = Catalog.silver(
    Tables.TRANSACTION
)


# ==========================================================
# Target
# ==========================================================

GOLD_DAILY_TRANSACTION_SUMMARY = Catalog.gold(
    Tables.DAILY_TRANSACTION_SUMMARY
)


# ==========================================================
# Daily Transaction Summary
# ==========================================================

@dp.materialized_view(
    name=GOLD_DAILY_TRANSACTION_SUMMARY,
    comment=(
        "Daily transaction summary by "
        "transaction type, status, channel "
        "and currency"
    ),
)
def daily_transaction_summary():

    # IMPORTANT:
    # Use dp.read so Lakeflow identifies
    # Silver -> Gold dependency.

    df = dp.read(
        SILVER_TRANSACTION
    )

    return build_daily_transaction_summary(
        df
    )