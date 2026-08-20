"""
Customer KYC Silver flow constants.
"""

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables


# ==========================================================
# Bronze Source
# ==========================================================

BRONZE_CUSTOMER_KYC = Catalog.bronze(
    Tables.CUSTOMER_KYC
)


# ==========================================================
# Silver Target
# ==========================================================

SILVER_CUSTOMER_KYC = Catalog.silver(
    Tables.CUSTOMER_KYC
)


# ==========================================================
# Quarantine Target
# ==========================================================

QUARANTINE_CUSTOMER_KYC = Catalog.quarantine(
    Tables.CUSTOMER_KYC_QUARANTINE
)