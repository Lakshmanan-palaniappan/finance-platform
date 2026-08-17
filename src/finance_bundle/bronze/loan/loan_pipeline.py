from pyspark import pipelines as dp

from finance_bundle.bronze.loan.loan_ingestion import (
    read_loan_data,
)

from finance_bundle.bronze.loan.loan_cdc_ingestion import (
    read_loan_cdc_data,
)

from finance_bundle.common.catalog import (
    Catalog,
)

from finance_bundle.common.table_names import (
    Tables,
)


# ==========================================================
# LOAN MASTER
# ==========================================================

@dp.table(
    name=Catalog.bronze(Tables.LOAN),
    comment="Bronze Loan master data",
)
@dp.expect(
    "loan_id_not_null",
    "loan_id IS NOT NULL",
)
def loan_bronze():

    return read_loan_data()


# ==========================================================
# LOAN CDC
# ==========================================================

@dp.table(
    name=Catalog.bronze(Tables.LOAN_CDC),
    comment="Bronze Loan CDC events",
)
@dp.expect(
    "cdc_event_id_not_null",
    "event_id IS NOT NULL",
)
@dp.expect(
    "cdc_loan_id_not_null",
    "loan_id IS NOT NULL",
)
@dp.expect(
    "cdc_operation_valid",
    "operation IN ('insert', 'update', 'delete')",
)
def loan_cdc_bronze():

    return read_loan_cdc_data()