"""
Loan Silver Lakeflow SDP Pipeline.

Bronze Loan
       +
Bronze Loan CDC
       |
       v
loan_cdc_source
       |
       v
Silver Loan SCD2
"""

from pyspark import pipelines as dp

from pyspark.sql import functions as F

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables

from finance_bundle.silver.loan.loan_transform import (
    transform_loan,
    add_loan_validation,
    get_quarantine_records,
    prepare_loan_cdc,
)


# ==========================================================
# Table Names
# ==========================================================

BRONZE_LOAN = Catalog.bronze(
    Tables.LOAN
)

BRONZE_LOAN_CDC = Catalog.bronze(
    Tables.LOAN_CDC
)

SILVER_LOAN = Catalog.silver(
    Tables.LOAN
)

SILVER_LOAN_QUARANTINE = Catalog.silver(
    Tables.LOAN_QUARANTINE
)


# ==========================================================
# Loan CDC Source
#
# EXACT SAME PATTERN AS ACCOUNT
# ==========================================================

@dp.temporary_view(
    name="loan_cdc_source",
)
def loan_cdc_source():

    # ------------------------------------------------------
    # Read Loan Master
    #
    # IMPORTANT:
    # This is BATCH / STATIC.
    # ------------------------------------------------------

    loan_df = dp.read(
        BRONZE_LOAN
    )

    # ------------------------------------------------------
    # Read Loan CDC
    #
    # IMPORTANT:
    # This is STREAMING.
    # ------------------------------------------------------

    cdc_df = dp.read_stream(
        BRONZE_LOAN_CDC
    )

    # ------------------------------------------------------
    # Prepare Complete CDC Record
    # ------------------------------------------------------

    return prepare_loan_cdc(
        cdc_df,
        loan_df,
    )


# ==========================================================
# Silver Loan SCD Type 2 Table
# ==========================================================

dp.create_streaming_table(

    name=SILVER_LOAN,

    comment=(
        "Silver Loan SCD Type 2 table"
    ),
)


# ==========================================================
# AUTO CDC
# ==========================================================

dp.create_auto_cdc_flow(

    target=SILVER_LOAN,

    source="loan_cdc_source",

    # ------------------------------------------------------
    # Business Key
    # ------------------------------------------------------

    keys=[
        "loan_id",
    ],

    # ------------------------------------------------------
    # CDC Ordering
    #
    # Account uses batch_id.
    # Follow the same pattern here.
    # ------------------------------------------------------

    sequence_by=F.col(
        "_batch_id"
    ),

    # ------------------------------------------------------
    # Delete
    # ------------------------------------------------------

    apply_as_deletes=(
        F.col(
            "_operation"
        )
        ==
        F.lit(
            "delete"
        )
    ),

    # ------------------------------------------------------
    # Remove CDC Metadata
    # ------------------------------------------------------

    except_column_list=[

        "_operation",

        "_batch_id",

        "_event_id",

        "_entity",

        "_event_timestamp",

        "_change_timestamp",
    ],

    # ------------------------------------------------------
    # SCD Type 2
    # ------------------------------------------------------

    stored_as_scd_type=2,
)


# ==========================================================
# LOAN QUARANTINE
# ==========================================================

@dp.table(

    name=SILVER_LOAN_QUARANTINE,

    comment=(
        "Loan records rejected "
        "during Silver validation"
    ),
)
def loan_quarantine():

    df = dp.read_stream(
        BRONZE_LOAN
    )

    df = transform_loan(
        df
    )

    df = add_loan_validation(
        df
    )

    return get_quarantine_records(
        df
    )