"""
Account Silver flow constants.
"""

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables


# ==========================================================
# Bronze Sources
# ==========================================================

BRONZE_ACCOUNT = Catalog.bronze(
    Tables.ACCOUNT
)

BRONZE_ACCOUNT_CDC = Catalog.bronze(
    Tables.ACCOUNT_CDC
)


# ==========================================================
# Silver Target
# ==========================================================

SILVER_ACCOUNT = Catalog.silver(
    Tables.ACCOUNT
)