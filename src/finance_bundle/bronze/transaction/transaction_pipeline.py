from pyspark import pipelines as dp

from finance_bundle.bronze.transaction.transaction_ingestion import (
    read_transaction_data,
)

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables


# ==========================================================
# Bronze Transaction
# ==========================================================

BRONZE_TRANSACTION = Catalog.bronze(
    Tables.TRANSACTION
)


@dp.table(
    name=BRONZE_TRANSACTION,
    comment=(
        "Bronze transaction streaming table "
        "ingested from the transaction landing "
        "zone using Auto Loader"
    ),
)
@dp.expect(
    "transaction_id_not_null",
    "transaction_id IS NOT NULL",
)
@dp.expect(
    "account_id_not_null",
    "account_id IS NOT NULL",
)
def transaction_bronze():

    return read_transaction_data()