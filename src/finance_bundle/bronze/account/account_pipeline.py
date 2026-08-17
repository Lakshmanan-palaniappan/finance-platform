from pyspark import pipelines as dp

from finance_bundle.bronze.account.account_ingestion import (
    read_account_data,
)

from finance_bundle.bronze.account.account_cdc_ingestion import (
    read_account_cdc_data,
)

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables


# ==========================================================
# ACCOUNT MASTER
# ==========================================================


@dp.table(
    name=Catalog.bronze(
        Tables.ACCOUNT
    ),
    comment="Bronze Account master data",
)
@dp.expect(
    "account_id_not_null",
    "account_id IS NOT NULL",
)
@dp.expect(
    "account_number_not_null",
    "account_number IS NOT NULL",
)
@dp.expect(
    "customer_id_not_null",
    "customer_id IS NOT NULL",
)
def account_bronze():

    return read_account_data()


# ==========================================================
# ACCOUNT CDC
# ==========================================================


@dp.table(
    name=Catalog.bronze(
        Tables.ACCOUNT_CDC
    ),
    comment="Bronze Account CDC events",
)
@dp.expect(
    "cdc_event_id_not_null",
    "event_id IS NOT NULL",
)
@dp.expect(
    "cdc_account_id_not_null",
    "account_id IS NOT NULL",
)
@dp.expect(
    "cdc_operation_valid",
    "operation IN ('insert', 'update', 'delete')",
)
def account_cdc_bronze():

    return read_account_cdc_data()