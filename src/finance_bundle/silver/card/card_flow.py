from pyspark import pipelines as dp

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables

from finance_bundle.silver.card.card_transform import (
    transform_card_master,
    transform_card_cdc,
)


# ==========================================================
# CARD MASTER CLEAN
# ==========================================================

@dp.temporary_view(
    name="card_master_clean"
)
@dp.expect_or_drop(
    "valid_card_id",
    "card_id IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_account_id",
    "account_id IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_customer_id",
    "customer_id IS NOT NULL"
)
def card_master_clean():

    df = spark.readStream.table(
        Catalog.bronze(Tables.CARD)
    )

    return transform_card_master(df)


# ==========================================================
# CARD CDC CLEAN
# ==========================================================

@dp.temporary_view(
    name="card_cdc_clean"
)
@dp.expect_or_drop(
    "valid_event_id",
    "event_id IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_card_id",
    "card_id IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_operation",
    "operation IN ('insert', 'update', 'delete')"
)
def card_cdc_clean():

    df = spark.readStream.table(
        Catalog.bronze(Tables.CARD_CDC)
    )

    return transform_card_cdc(df)


# ==========================================================
# CARD CDC QUARANTINE
# ==========================================================

@dp.table(
    name=Catalog.silver("card_quarantine"),
    comment="Quarantined invalid Card CDC records"
)
def card_quarantine():

    df = spark.readStream.table(
        Catalog.bronze(Tables.CARD_CDC)
    )

    df = transform_card_cdc(df)

    return df.filter(
        """
        event_id IS NULL
        OR card_id IS NULL
        OR operation NOT IN ('insert', 'update', 'delete')
        """
    )