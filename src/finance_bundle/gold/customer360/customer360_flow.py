"""
Customer 360 Gold flow constants.
"""

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables


# ==========================================================
# Silver Source
# ==========================================================

SILVER_CUSTOMER = Catalog.silver(
    Tables.CUSTOMER
)


# ==========================================================
# Gold Target
# ==========================================================

GOLD_CUSTOMER360 = Catalog.gold(
    Tables.CUSTOMER360
)