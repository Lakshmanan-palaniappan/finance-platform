from pyspark import pipelines as dp

from finance_bundle.bronze.exchange_rate.exchange_rate_ingestion import (
    read_exchange_rate_data,
)

from finance_bundle.common.catalog import (
    Catalog,
)

from finance_bundle.common.table_names import (
    Tables,
)


@dp.table(
    name=Catalog.bronze(Tables.EXCHANGE_RATE),
    comment="Bronze Exchange Rate Table",
)
def exchange_rate_bronze():

    return read_exchange_rate_data()
