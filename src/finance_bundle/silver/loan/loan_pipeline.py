from pyspark import pipelines as dp

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables

from finance_bundle.silver.loan.loan_transform import (
    transform_loan,
    add_loan_validation,
    get_valid_records,
    get_quarantine_records,
    prepare_loan_cdc,
)


# ==========================================================
# SILVER LOAN
# ==========================================================

@dp.table(
    name=Catalog.silver(Tables.LOAN),
    comment="Cleaned and validated Silver Loan data"
)
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
def silver_loan():

    bronze_df = (
        spark.readStream
        .table(
            Catalog.bronze(Tables.LOAN)
        )
    )

    transformed_df = transform_loan(
        bronze_df
    )

    validated_df = add_loan_validation(
        transformed_df
    )

    return get_valid_records(
        validated_df
    )


# ==========================================================
# LOAN QUARANTINE
# ==========================================================

@dp.table(
    name=Catalog.silver(Tables.LOAN_QUARANTINE),
    comment="Loan records rejected during Silver validation"
)
def silver_loan_quarantine():

    bronze_df = (
        spark.readStream
        .table(
            Catalog.bronze(Tables.LOAN)
        )
    )

    transformed_df = transform_loan(
        bronze_df
    )

    validated_df = add_loan_validation(
        transformed_df
    )

    return get_quarantine_records(
        validated_df
    )


# ==========================================================
# PREPARED LOAN CDC
# ==========================================================

@dp.temporary_view(
    name="loan_cdc_prepared"
)
def loan_cdc_prepared():

    cdc_df = (
        spark.readStream
        .table(
            Catalog.bronze(Tables.LOAN_CDC)
        )
    )

    loan_df = (
        spark.readStream
        .table(
            Catalog.bronze(Tables.LOAN)
        )
    )

    cleaned_loan_df = transform_loan(
        loan_df
    )

    return prepare_loan_cdc(
        cdc_df,
        cleaned_loan_df
    )