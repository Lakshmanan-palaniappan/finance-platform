from pyspark import pipelines as dp

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables

from finance_bundle.silver.exchange_rate.exchange_rate_flow import (
    exchange_rate_silver_flow,
    exchange_rate_quarantine_flow,
)


# ==========================================================
# SILVER EXCHANGE RATE
# ==========================================================

@dp.table(
    name=Catalog.silver(
        Tables.EXCHANGE_RATE
    ),
    comment="Cleaned and validated Silver Exchange Rate table",
)

@dp.expect(
    "valid_base_currency",
    "base_currency IS NOT NULL"
)

@dp.expect(
    "valid_target_currency",
    "target_currency IS NOT NULL"
)

@dp.expect(
    "valid_exchange_rate",
    "exchange_rate IS NOT NULL AND exchange_rate > 0"
)

@dp.expect(
    "valid_effective_date",
    "effective_date IS NOT NULL"
)

@dp.expect(
    "valid_currency_pair",
    "base_currency <> target_currency"
)

def silver_exchange_rate():

    return exchange_rate_silver_flow()


# ==========================================================
# EXCHANGE RATE QUARANTINE
# ==========================================================

@dp.table(
    name=Catalog.silver(
        "exchange_rate_quarantine"
    ),
    comment="Quarantine table for invalid Exchange Rate records",
)

def exchange_rate_quarantine():

    return exchange_rate_quarantine_flow()