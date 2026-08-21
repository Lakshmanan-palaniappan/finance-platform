from pyspark import pipelines as dp

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables

from finance_bundle.gold.exchange_rate.exchange_rate_transform import (
    create_exchange_rate_summary,
)


# ==========================================================
# GOLD EXCHANGE RATE SUMMARY
# ==========================================================

@dp.materialized_view(
    name=Catalog.gold(
        Tables.EXCHANGE_RATE_SUMMARY
    ),
    comment="""
    Gold Exchange Rate Summary.

    Grain:
        One row per base_currency and target_currency.

    KPI Definitions:

    latest_exchange_rate:
        Most recent exchange rate for the currency pair.

    average_exchange_rate:
        Average exchange rate across all valid observations.

    minimum_exchange_rate:
        Lowest observed exchange rate.

    maximum_exchange_rate:
        Highest observed exchange rate.

    exchange_rate_volatility:
        Standard deviation of observed exchange rates.

    rate_range:
        Difference between maximum and minimum exchange rate.

    observation_count:
        Number of valid exchange rate observations.

    latest_rate_change:
        Difference between the latest rate and the
        immediately preceding rate.

    latest_rate_change_percentage:
        Percentage change between the latest rate and
        the immediately preceding rate.

    rate_change_direction:
        Indicates whether the latest rate increased,
        decreased, or remained unchanged.

    business_rule_status:
        VALID when exchange rate and currency-pair
        business rules are satisfied.
    """,

    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def gold_exchange_rate_summary():

    # ======================================================
    # Read Silver Exchange Rate
    # ======================================================

    silver_df = dp.read(
        Catalog.silver(
            Tables.EXCHANGE_RATE
        )
    )

    # ======================================================
    # Transform Silver → Gold
    # ======================================================

    return create_exchange_rate_summary(
        silver_df
    )