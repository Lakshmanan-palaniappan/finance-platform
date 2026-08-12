from pyspark import pipelines as dp

from finance_bundle.bronze.customer.customer_ingestion import (
    read_customer_data,
)

from finance_bundle.common.catalog import (
    Catalog,
)

from finance_bundle.common.table_names import (
    Tables,
)


@dp.table(
    name=Catalog.bronze(Tables.CUSTOMER),
    comment="Bronze Customer Table",
)
def customer_bronze():

    return read_customer_data()