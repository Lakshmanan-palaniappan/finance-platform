"""
Customer KYC Gold transformations.

Creates:

    Customer KYC Compliance Summary
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ==========================================================
# Customer-Level KYC Status
# ==========================================================

def build_customer_level_kyc(
    kyc_df: DataFrame,
) -> DataFrame:

    return (
        kyc_df
        .groupBy(
            "customer_id",
        )
        .agg(

            # --------------------------------------------------
            # Any PAN successfully verified
            # --------------------------------------------------

            F.max(
                F.when(
                    F.col("pan_verified") == True,
                    1,
                ).otherwise(0)
            ).alias(
                "pan_verified_flag"
            ),

            # --------------------------------------------------
            # Any Aadhaar successfully verified
            # --------------------------------------------------

            F.max(
                F.when(
                    F.col("aadhaar_verified") == True,
                    1,
                ).otherwise(0)
            ).alias(
                "aadhaar_verified_flag"
            ),

            # --------------------------------------------------
            # Any VERIFIED KYC record
            # --------------------------------------------------

            F.max(
                F.when(
                    F.col("kyc_status") == "VERIFIED",
                    1,
                ).otherwise(0)
            ).alias(
                "verified_flag"
            ),

            # --------------------------------------------------
            # Any PENDING KYC record
            # --------------------------------------------------

            F.max(
                F.when(
                    F.col("kyc_status") == "PENDING",
                    1,
                ).otherwise(0)
            ).alias(
                "pending_flag"
            ),

            # --------------------------------------------------
            # Any REJECTED KYC record
            # --------------------------------------------------

            F.max(
                F.when(
                    F.col("kyc_status") == "REJECTED",
                    1,
                ).otherwise(0)
            ).alias(
                "rejected_flag"
            ),
        )
    )


# ==========================================================
# Compliance Summary
# ==========================================================

def build_compliance_summary(
    kyc_df: DataFrame,
) -> DataFrame:

    customer_df = build_customer_level_kyc(
        kyc_df
    )

    return customer_df.agg(

        # --------------------------------------------------
        # Total Customers
        # --------------------------------------------------

        F.count(
            "*"
        ).alias(
            "total_customers"
        ),

        # --------------------------------------------------
        # Verified Customers
        # --------------------------------------------------

        F.sum(
            "verified_flag"
        ).alias(
            "verified_customers"
        ),

        # --------------------------------------------------
        # Pending Customers
        # --------------------------------------------------

        F.sum(
            "pending_flag"
        ).alias(
            "pending_customers"
        ),

        # --------------------------------------------------
        # Rejected Customers
        # --------------------------------------------------

        F.sum(
            "rejected_flag"
        ).alias(
            "rejected_customers"
        ),

        # --------------------------------------------------
        # PAN Verified Customers
        # --------------------------------------------------

        F.sum(
            "pan_verified_flag"
        ).alias(
            "pan_verified_customers"
        ),

        # --------------------------------------------------
        # Aadhaar Verified Customers
        # --------------------------------------------------

        F.sum(
            "aadhaar_verified_flag"
        ).alias(
            "aadhaar_verified_customers"
        ),

        # --------------------------------------------------
        # Fully Compliant Customers
        #
        # A customer is considered fully compliant when:
        #
        #   1. PAN is verified
        #   2. Aadhaar is verified
        #   3. At least one KYC record is VERIFIED
        #
        # Address verification and document expiry are NOT
        # included because those fields are not present in
        # the current KYC source.
        # --------------------------------------------------

        F.sum(
            F.when(
                (
                    F.col("pan_verified_flag") == 1
                )
                &
                (
                    F.col("aadhaar_verified_flag") == 1
                )
                &
                (
                    F.col("verified_flag") == 1
                ),
                1,
            ).otherwise(0)
        ).alias(
            "fully_compliant_customers"
        ),
    )


# ==========================================================
# Compliance Rates
# ==========================================================

def add_compliance_rates(
    df: DataFrame,
) -> DataFrame:

    return (
        df

        # --------------------------------------------------
        # PAN Verification Rate
        # --------------------------------------------------

        .withColumn(
            "pan_verification_rate",
            F.when(
                F.col("total_customers") > 0,
                F.round(
                    (
                        F.col("pan_verified_customers")
                        /
                        F.col("total_customers")
                    )
                    * 100,
                    2,
                ),
            ).otherwise(
                F.lit(0.0)
            ),
        )

        # --------------------------------------------------
        # Aadhaar Verification Rate
        # --------------------------------------------------

        .withColumn(
            "aadhaar_verification_rate",
            F.when(
                F.col("total_customers") > 0,
                F.round(
                    (
                        F.col("aadhaar_verified_customers")
                        /
                        F.col("total_customers")
                    )
                    * 100,
                    2,
                ),
            ).otherwise(
                F.lit(0.0)
            ),
        )

        # --------------------------------------------------
        # Full Compliance Rate
        # --------------------------------------------------

        .withColumn(
            "full_compliance_rate",
            F.when(
                F.col("total_customers") > 0,
                F.round(
                    (
                        F.col("fully_compliant_customers")
                        /
                        F.col("total_customers")
                    )
                    * 100,
                    2,
                ),
            ).otherwise(
                F.lit(0.0)
            ),
        )
    )


# ==========================================================
# Final Gold Transformation
# ==========================================================

def transform_customer_kyc_gold(
    kyc_df: DataFrame,
) -> DataFrame:

    compliance_df = build_compliance_summary(
        kyc_df
    )

    return add_compliance_rates(
        compliance_df
    )