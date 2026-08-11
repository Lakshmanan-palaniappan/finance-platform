from pyspark import pipelines as dp

from finance_bundle.bronze.card.card_ingestion import (
    read_card_data,
)

from finance_bundle.common.catalog import (
    Catalog,
)

from finance_bundle.common.table_names import (
    Tables,
)


@dp.table(
    name=Catalog.bronze(Tables.CARD),
    comment="Bronze Card Table",
)
def card_bronze():

    return read_card_data()