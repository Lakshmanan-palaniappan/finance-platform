from pyspark.sql import DataFrame, SparkSession

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables

from finance_bundle.silver.exchange_rate.exchange_rate_transform import (
    transform_exchange_rate,
    transform_exchange_rate_quarantine,
)


# ==========================================================
# GET SPARK SESSION
# ==========================================================

def get_spark():
    """
    Gets the active Spark session used by the SDP pipeline.
    """

    spark = SparkSession.getActiveSession()

    if spark is None:
        raise RuntimeError(
            "No active Spark session found."
        )

    return spark


# ==========================================================
# READ BRONZE EXCHANGE RATE
# ==========================================================

def read_bronze_exchange_rate() -> DataFrame:
    """
    Reads the Bronze Exchange Rate streaming table.
    """

    spark = get_spark()

    return spark.readStream.table(
        Catalog.bronze(
            Tables.EXCHANGE_RATE
        )
    )


# ==========================================================
# SILVER FLOW
# ==========================================================

def exchange_rate_silver_flow() -> DataFrame:
    """
    Bronze
        ↓
    Clean
        ↓
    Validate
        ↓
    Remove invalid records
        ↓
    Deduplicate
        ↓
    Silver
    """

    bronze_df = read_bronze_exchange_rate()

    return transform_exchange_rate(
        bronze_df
    )


# ==========================================================
# QUARANTINE FLOW
# ==========================================================

def exchange_rate_quarantine_flow() -> DataFrame:
    """
    Bronze
        ↓
    Clean
        ↓
    Validate
        ↓
    Invalid records
        ↓
    Quarantine
    """

    bronze_df = read_bronze_exchange_rate()

    return transform_exchange_rate_quarantine(
        bronze_df
    )