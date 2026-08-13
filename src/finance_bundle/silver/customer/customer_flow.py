"""
Customer Silver flow constants.
"""

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables


# ==========================================================
# Bronze Sources
# ==========================================================

BRONZE_CUSTOMER = Catalog.bronze(
    Tables.CUSTOMER
)

BRONZE_CUSTOMER_CDC = Catalog.bronze(
    Tables.CUSTOMER_CDC
)


# ==========================================================
# Silver Target
# ==========================================================

SILVER_CUSTOMER = Catalog.silver(
    Tables.CUSTOMER
)