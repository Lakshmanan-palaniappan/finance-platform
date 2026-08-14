"""
Customer 360 Gold Lakeflow SDP pipeline.
"""

from pyspark import pipelines as dp

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables

from finance_bundle.gold.customer360.customer360_transform import (
    build_customer360,
)


# ==========================================================
# Tables
# ==========================================================

SILVER_CUSTOMER = Catalog.silver(
    Tables.CUSTOMER
)

GOLD_CUSTOMER360 = Catalog.gold(
    Tables.CUSTOMER360
)


# ==========================================================
# Customer 360
# ==========================================================

@dp.materialized_view(
    name=GOLD_CUSTOMER360,

    comment=(
        "Gold Customer 360 analytical view "
        "built from Silver Customer SCD Type 2"
    ),
)
def customer360():

    customer_df = dp.read(
        SILVER_CUSTOMER
    )

    return build_customer360(
        customer_df
    )