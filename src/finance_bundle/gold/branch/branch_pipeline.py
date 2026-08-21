from pyspark import pipelines as dp

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables

from finance_bundle.gold.branch.branch_transform import (
    transform_branch_gold,
)


# ==========================================================
# Gold Output
# ==========================================================

GOLD_BRANCH_PERFORMANCE = Catalog.gold(
    "branch_performance"
)


# ==========================================================
# Gold Branch Performance
# ==========================================================

@dp.materialized_view(
    name=GOLD_BRANCH_PERFORMANCE,
    comment=(
        "Gold Branch Performance containing "
        "branch-level business metrics and KPIs."
    ),
)
def branch_performance():

    return transform_branch_gold()