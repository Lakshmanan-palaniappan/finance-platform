"""
Customer KYC Silver Lakeflow SDP Pipeline.

Bronze Customer KYC
        |
        +----------------------+
        |                      |
        v                      v
Silver Customer KYC      KYC Quarantine
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables

from finance_bundle.silver.customer_kyc.customer_kyc_transform import (
    normalize_customer_kyc,
)


# ==========================================================
# Table Names
# ==========================================================

BRONZE_CUSTOMER_KYC = Catalog.bronze(
    Tables.CUSTOMER_KYC
)

SILVER_CUSTOMER_KYC = Catalog.silver(
    Tables.CUSTOMER_KYC
)

QUARANTINE_CUSTOMER_KYC = Catalog.quarantine(
    Tables.CUSTOMER_KYC_QUARANTINE
)


# ==========================================================
# Silver Customer KYC
# ==========================================================

@dp.materialized_view(
    name=SILVER_CUSTOMER_KYC,
    comment=(
        "Silver Customer KYC "
        "cleansed and standardized data"
    ),
)
@dp.expect(
    "customer_id_not_null",
    "customer_id IS NOT NULL",
)
@dp.expect(
    "kyc_id_not_null",
    "kyc_id IS NOT NULL",
)
@dp.expect(
    "kyc_status_valid",
    """
    kyc_status IN (
        'VERIFIED',
        'PENDING',
        'REJECTED'
    )
    """,
)
def customer_kyc_silver():

    df = dp.read(
        BRONZE_CUSTOMER_KYC
    )

    df = normalize_customer_kyc(
        df
    )

    # ------------------------------------------------------
    # Keep only valid Silver records
    # ------------------------------------------------------

    return df.filter(
        F.col("customer_id").isNotNull()
        &
        F.col("kyc_id").isNotNull()
        &
        F.col("kyc_status").isin(
            "VERIFIED",
            "PENDING",
            "REJECTED",
        )
    )


# ==========================================================
# KYC Quarantine
# ==========================================================

@dp.materialized_view(
    name=QUARANTINE_CUSTOMER_KYC,
    comment=(
        "Customer KYC records that "
        "failed Silver validation"
    ),
)
def customer_kyc_quarantine():

    df = normalize_customer_kyc(
        dp.read(
            BRONZE_CUSTOMER_KYC
        )
    )

    # ------------------------------------------------------
    # Keep only invalid records
    # ------------------------------------------------------

    return df.filter(
        F.col("customer_id").isNull()
        |
        F.col("kyc_id").isNull()
        |
        F.col("kyc_status").isNull()
        |
        ~F.col("kyc_status").isin(
            "VERIFIED",
            "PENDING",
            "REJECTED",
        )
    )