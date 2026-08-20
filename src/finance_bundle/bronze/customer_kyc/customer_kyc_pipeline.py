from pyspark import pipelines as dp

from finance_bundle.bronze.customer_kyc.customer_kyc_ingestion import (
    read_customer_kyc_data,
)

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables


# ==========================================================
# Customer KYC Bronze
# ==========================================================

@dp.table(
    name=Catalog.bronze(
        Tables.CUSTOMER_KYC
    ),

    comment="Bronze Customer KYC master data",
)
@dp.expect(
    "customer_id_not_null",
    "customer_id IS NOT NULL",
)
def customer_kyc_bronze():

    return read_customer_kyc_data()