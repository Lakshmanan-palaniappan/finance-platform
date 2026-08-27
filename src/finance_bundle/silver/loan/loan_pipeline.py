from pyspark import pipelines as dp
from pyspark.sql import functions as F

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables
from finance_bundle.silver.loan.loan_transform import (
    transform_loan,
    prepare_loan_cdc,
)


# ==========================================================
# TABLE NAMES
# ==========================================================

BRONZE_LOAN = Catalog.bronze(Tables.LOAN)
BRONZE_LOAN_CDC = Catalog.bronze(Tables.LOAN_CDC)

SILVER_LOAN = Catalog.silver(Tables.LOAN)


# ==========================================================
# LOAN MASTER SNAPSHOT
#
# 81 records from Bronze Loan.
#
# This is kept separate from CDC processing.
# ==========================================================

@dp.temporary_view(
    name="loan_master_snapshot"
)
def loan_master_snapshot():

    df = dp.read(BRONZE_LOAN)

    return transform_loan(df)


# ==========================================================
# CDC SOURCE
#
# CDC is STREAMING.
# Master snapshot is STATIC.
#
# DO NOT use read_stream() for the master table here.
# ==========================================================

@dp.temporary_view(
    name="loan_cdc_source"
)
def loan_cdc_source():

    cdc_df = dp.read_stream(
        BRONZE_LOAN_CDC
    )

    master_df = dp.read(
        BRONZE_LOAN
    )

    return prepare_loan_cdc(
        cdc_df,
        master_df
    )


# ==========================================================
# SILVER LOAN
# ==========================================================

dp.create_streaming_table(
    name=SILVER_LOAN,
    comment="Silver Loan SCD Type 2 table"
)


# ==========================================================
# AUTO CDC
# ==========================================================

dp.create_auto_cdc_flow(

    target=SILVER_LOAN,

    source="loan_cdc_source",

    keys=[
        "loan_id"
    ],

    sequence_by=F.col(
        "_change_timestamp"
    ),

    apply_as_deletes=(
        F.col("_operation") == "delete"
    ),

    except_column_list=[
        "_operation",
        "_batch_id",
        "_event_id",
        "_entity",
        "_event_timestamp",
        "_change_timestamp",
    ],

    stored_as_scd_type=2,
)