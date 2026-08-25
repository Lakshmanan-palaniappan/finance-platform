from pyspark import pipelines as dp

from pyspark.sql.functions import (
    col,
    expr,
)

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
# NORMAL LOAN VALIDATION
# ==========================================================

@dp.table(
    name=Catalog.silver(
        Tables.LOAN_VALIDATED
    ),
    comment="Cleaned and validated Loan records",
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
def loan_validated():

    df = dp.read_stream(
        Catalog.bronze(
            Tables.LOAN
        )
    )

    df = transform_loan(df)

    df = add_loan_validation(df)

    return get_valid_records(df)


# ==========================================================
# QUARANTINE
# ==========================================================

@dp.table(
    name=Catalog.silver(
        Tables.LOAN_QUARANTINE
    ),
    comment="Rejected Loan records",
)
def loan_quarantine():

    df = dp.read_stream(
        Catalog.bronze(
            Tables.LOAN
        )
    )

    df = transform_loan(df)

    df = add_loan_validation(df)

    return get_quarantine_records(df)


# ==========================================================
# CDC PREPARATION
# ==========================================================

@dp.temporary_view(
    name="loan_cdc_prepared",
)
def loan_cdc_prepared():

    cdc_df = dp.read_stream(
        Catalog.bronze(
            Tables.LOAN_CDC
        )
    )

    loan_df = dp.read_stream(
        Catalog.bronze(
            Tables.LOAN
        )
    )

    return prepare_loan_cdc(
        cdc_df,
        loan_df,
    )


# ==========================================================
# SILVER LOAN SCD2 TARGET
# ==========================================================

dp.create_streaming_table(
    name=Catalog.silver(
        Tables.LOAN
    ),
    comment="Silver Loan SCD Type 2 table",
)


# ==========================================================
# CDC → SILVER
# ==========================================================

dp.create_auto_cdc_flow(

    target=Catalog.silver(
        Tables.LOAN
    ),

    source="loan_cdc_prepared",

    keys=[
        "loan_id"
    ],

    sequence_by=col(
        "change_timestamp"
    ),

    # ------------------------------------------------------
    # IMPORTANT FOR PARTIAL CDC
    # ------------------------------------------------------

    ignore_null_updates=True,

    apply_as_deletes=expr(
        "operation = 'delete'"
    ),

    except_column_list=[
        "operation",
        "event_id",
        "batch_id",
        "source_system",
        "event_timestamp",
        "change_timestamp",
    ],

    stored_as_scd_type=2,
)