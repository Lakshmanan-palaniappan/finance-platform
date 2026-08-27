from pyspark import pipelines as dp

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables

from finance_bundle.gold.card.card_transform import (
    transform_card_gold,
)


# ==========================================================
# GOLD CARD MATERIALIZED VIEW
# ==========================================================

@dp.materialized_view(
    name=Catalog.gold(
        Tables.CARD
    ),

    comment="""
    Gold Card analytics table containing
    business metrics, KPIs and aggregations
    for current Card records.
    """,
)
def gold_card():

    return transform_card_gold()