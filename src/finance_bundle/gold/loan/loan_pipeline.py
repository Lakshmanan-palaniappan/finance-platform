from pyspark import pipelines as dp

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables

from finance_bundle.gold.loan.loan_transform import (
    transform_loan_gold,
)


# ==========================================================
# TABLES
# ==========================================================

SILVER_LOAN = Catalog.silver(
    Tables.LOAN
)

GOLD_LOAN = Catalog.gold(
    Tables.LOAN
)


# ==========================================================
# GOLD LOAN
# ==========================================================

@dp.materialized_view(

    name=GOLD_LOAN,

    comment="""
    Gold Loan business-ready table.

    Contains:
    - Loan outstanding percentage
    - EMI completion percentage
    - Loan age
    - Risk category
    - Outstanding ratio
    - Loan performance
    """,
)

@dp.expect(
    "loan_id_not_null",
    "loan_id IS NOT NULL",
)

@dp.expect(
    "customer_id_not_null",
    "customer_id IS NOT NULL",
)

@dp.expect(
    "branch_id_not_null",
    "branch_id IS NOT NULL",
)

@dp.expect(
    "loan_amount_valid",
    "loan_amount >= 0",
)

@dp.expect(
    "interest_rate_valid",
    "interest_rate >= 0",
)

@dp.expect(
    "tenure_valid",
    "tenure_years > 0",
)

@dp.expect(
    "outstanding_balance_valid",
    "outstanding_balance >= 0",
)

@dp.expect(
    "outstanding_not_greater_than_loan",
    "outstanding_balance <= loan_amount",
)
def gold_loan():

    # ======================================================
    # READ SILVER
    # ======================================================

    df = dp.read(
        SILVER_LOAN
    )

    # ======================================================
    # GOLD TRANSFORMATION
    # ======================================================

    return transform_loan_gold(
        df
    )