from pyspark import pipelines as dp

from finance_bundle.bronze.card.card_ingestion import (
    read_card_data,
)

from finance_bundle.bronze.card.card_cdc_ingestion import (
    read_card_cdc_data,
)

from finance_bundle.common.catalog import (
    Catalog,
)

from finance_bundle.common.table_names import (
    Tables,
)


# ==========================================================
# CARD MASTER
# ==========================================================


@dp.table(
    name=Catalog.bronze(Tables.CARD),
    comment="Bronze Card master data",
)
@dp.expect(
    "card_id_not_null",
    "card_id IS NOT NULL",
)
def card_bronze():

    return read_card_data()


# ==========================================================
# CARD CDC
# ==========================================================


@dp.table(
    name=Catalog.bronze(Tables.CARD_CDC),
    comment="Bronze Card CDC events",
)
@dp.expect(
    "cdc_event_id_not_null",
    "event_id IS NOT NULL",
)
@dp.expect(
    "cdc_card_id_not_null",
    "card_id IS NOT NULL",
)
@dp.expect(
    "cdc_operation_valid",
    "operation IN ('insert', 'update', 'delete')",
)
def card_cdc_bronze():

    return read_card_cdc_data()