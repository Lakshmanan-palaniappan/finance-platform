"""
Customer KYC Gold Lakeflow SDP Pipeline.

Silver Customer KYC
        |
        v
Compliance Summary
"""

from pyspark import pipelines as dp

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables

from finance_bundle.gold.customer_kyc.customer_kyc_transform import (
    build_compliance_summary,
)


# ==========================================================
# Tables
# ==========================================================

SILVER_CUSTOMER_KYC = Catalog.silver(
    Tables.CUSTOMER_KYC
)

GOLD_CUSTOMER_KYC_COMPLIANCE = Catalog.gold(
    Tables.CUSTOMER_KYC_COMPLIANCE
)


# ==========================================================
# Compliance Summary
# ==========================================================

@dp.materialized_view(
    name=GOLD_CUSTOMER_KYC_COMPLIANCE,

    comment=(
        "Gold Customer KYC "
        "Compliance Summary"
    ),
)
def customer_kyc_compliance_summary():

    kyc_df = dp.read(
        SILVER_CUSTOMER_KYC
    )

    return build_compliance_summary(
        kyc_df
    )