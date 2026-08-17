from pyspark import pipelines as dp

from finance_bundle.bronze.atm_transaction.atm_transaction_ingestion import (
    read_atm_transaction_data,
)

from finance_bundle.common.catalog import (
    Catalog,
)

from finance_bundle.common.table_names import (
    Tables,
)


@dp.table(
    name=Catalog.bronze(Tables.ATM_TRANSACTION),
    comment="Bronze ATM Transaction Table",
)
def atm_transaction_bronze():

    return read_atm_transaction_data()