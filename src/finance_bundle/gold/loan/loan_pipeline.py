from pyspark import pipelines as dp
from pyspark.sql import functions as F

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables

from finance_bundle.gold.loan.loan_transform import (
    transform_loan_gold
)


# ==========================================================
# GOLD LOAN
# ==========================================================

@dp.materialized_view(
    name=Catalog.gold(Tables.LOAN),
    comment="""
    Gold Loan table containing business-ready Loan data,
    business metrics, KPIs, risk classification and
    validated business rules.
    
    KPI Definitions:
    
    loan_outstanding_pct:
        Outstanding balance as a percentage of loan amount.
    
    emi_completion_pct:
        Percentage of EMIs already paid.
    
    outstanding_ratio_pct:
        Outstanding balance as a percentage of loan amount.
    
    risk_category:
        HIGH when loan is DEFAULTED or loan-to-income ratio >= 5.
        MEDIUM when loan-to-income ratio >= 3.
        LOW otherwise.
    
    loan_performance:
        CRITICAL for defaulted loans.
        GOOD when EMI completion >= 75%.
        WATCH when EMI completion >= 40%.
        AT_RISK otherwise.
    """
)

# ==========================================================
# DATA QUALITY EXPECTATIONS
# ==========================================================

@dp.expect(
    "loan_id_not_null",
    "loan_id IS NOT NULL"
)

@dp.expect(
    "customer_id_not_null",
    "customer_id IS NOT NULL"
)

@dp.expect(
    "branch_id_not_null",
    "branch_id IS NOT NULL"
)

@dp.expect(
    "loan_amount_valid",
    "loan_amount >= 0"
)

@dp.expect(
    "interest_rate_valid",
    "interest_rate >= 0"
)

@dp.expect(
    "tenure_valid",
    "tenure_years > 0"
)

@dp.expect(
    "outstanding_balance_valid",
    "outstanding_balance >= 0"
)

@dp.expect(
    "outstanding_not_greater_than_loan",
    "outstanding_balance <= loan_amount"
)

def gold_loan():

    # ======================================================
    # READ SILVER LOAN
    # ======================================================

    silver_df = (
        spark.read
        .table(
            Catalog.silver(Tables.LOAN)
        )
    )

    # ======================================================
    # SCD TYPE 2
    # ======================================================
    # Keep only the current version of each loan.
    #
    # IMPORTANT:
    # If your SCD2 implementation does not generate
    # _is_current, remove this section or replace it
    # with the actual current-record column.
    # ======================================================

    if "_is_current" in silver_df.columns:

        silver_df = silver_df.filter(
            F.col("_is_current") == True
        )

    # ======================================================
    # SILVER -> GOLD TRANSFORMATION
    # ======================================================

    gold_df = transform_loan_gold(
        silver_df
    )

    return gold_df