from pyspark import pipelines as dp

from finance_bundle.bronze.customer.customer_ingestion import (
    read_customer_data,
)

from finance_bundle.bronze.customer.customer_cdc_ingestion import (
    read_customer_cdc_data,
)

from finance_bundle.common.catalog import Catalog

from finance_bundle.common.table_names import Tables


# ==========================================================
# CUSTOMER MASTER
# ==========================================================


@dp.table(
    name=Catalog.bronze(Tables.CUSTOMER),
    comment="Bronze Customer master data",
)
@dp.expect(
    "customer_id_not_null",
    "customer_id IS NOT NULL",
)
def customer_bronze():

    return read_customer_data()


# ==========================================================
# CUSTOMER CDC
# ==========================================================


@dp.table(
    name=Catalog.bronze(Tables.CUSTOMER_CDC),
    comment="Bronze Customer CDC events",
)
@dp.expect(
    "cdc_event_id_not_null",
    "event_id IS NOT NULL",
)
@dp.expect(
    "cdc_customer_id_not_null",
    "customer_id IS NOT NULL",
)
@dp.expect(
    "cdc_operation_valid",
    "operation IN ('insert', 'update', 'delete')",
)
def customer_cdc_bronze():

    return read_customer_cdc_data()
