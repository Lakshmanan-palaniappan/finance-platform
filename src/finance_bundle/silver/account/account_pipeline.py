"""
Account Silver Lakeflow SDP Pipeline.

Bronze Account
       +
Bronze Account CDC
       |
       v
account_cdc_source
       |
       v
Silver Account SCD2
"""

from pyspark import pipelines as dp

from pyspark.sql import functions as F

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables

from finance_bundle.silver.account.account_transform import (
    normalize_account,
)


# ==========================================================
# Table Names
# ==========================================================


BRONZE_ACCOUNT = Catalog.bronze(
    Tables.ACCOUNT
)

BRONZE_ACCOUNT_CDC = Catalog.bronze(
    Tables.ACCOUNT_CDC
)

SILVER_ACCOUNT = Catalog.silver(
    Tables.ACCOUNT
)


# ==========================================================
# Account CDC Source
# ==========================================================


@dp.temporary_view(
    name="account_cdc_source",
)
def account_cdc_source():

    # ------------------------------------------------------
    # Read Account Master
    # ------------------------------------------------------

    account_df = dp.read(
        BRONZE_ACCOUNT
    )

    # ------------------------------------------------------
    # Read Account CDC
    # ------------------------------------------------------

    cdc_df = dp.read_stream(
        BRONZE_ACCOUNT_CDC
    )

    # ------------------------------------------------------
    # Normalize Account
    # ------------------------------------------------------

    account_df = normalize_account(
        account_df
    )

    # ------------------------------------------------------
    # Normalize Account Column Names
    # ------------------------------------------------------

    for column_name in account_df.columns:

        normalized = (
            column_name
            .strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
        )

        if normalized != column_name:

            account_df = account_df.withColumnRenamed(
                column_name,
                normalized,
            )

    # ------------------------------------------------------
    # Normalize CDC Column Names
    # ------------------------------------------------------

    for column_name in cdc_df.columns:

        normalized = (
            column_name
            .strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
        )

        if normalized != column_name:

            cdc_df = cdc_df.withColumnRenamed(
                column_name,
                normalized,
            )

    # ======================================================
    # Normalize CDC Values
    # ======================================================

    cdc_df = (
        cdc_df

        .withColumn(
            "entity",
            F.upper(
                F.trim(
                    F.col("entity")
                )
            ),
        )

        .withColumn(
            "operation",
            F.lower(
                F.trim(
                    F.col("operation")
                )
            ),
        )

        .withColumn(
            "account_id",
            F.trim(
                F.col("account_id")
            ),
        )

        .withColumn(
            "batch_id",
            F.trim(
                F.col("batch_id")
            ),
        )
    )

    # ======================================================
    # Remove Duplicate Account State
    # ======================================================

    account_df = account_df.dropDuplicates(
        ["account_id"]
    )

    # ======================================================
    # Join CDC Events With Account State
    # ======================================================

    joined_df = (

        cdc_df.alias("cdc")

        .join(
            account_df.alias("account"),

            F.col("cdc.account_id")
            ==
            F.col("account.account_id"),

            "left",
        )
    )

    # ======================================================
    # Create Full CDC Record
    # ======================================================

    return joined_df.select(

        # --------------------------------------------------
        # Account Business Columns
        # --------------------------------------------------

        F.col(
            "account.account_id"
        ).alias("account_id"),

        F.col(
            "account.account_number"
        ).alias("account_number"),

        F.col(
            "account.customer_id"
        ).alias("customer_id"),

        F.col(
            "account.branch_id"
        ).alias("branch_id"),

        F.col(
            "account.account_type"
        ).alias("account_type"),

        F.col(
            "account.balance"
        ).alias("balance"),

        F.col(
            "account.minimum_balance"
        ).alias("minimum_balance"),

        F.col(
            "account.interest_rate"
        ).alias("interest_rate"),

        F.col(
            "account.opened_date"
        ).alias("opened_date"),

        F.col(
            "account.account_status"
        ).alias("account_status"),

        # --------------------------------------------------
        # CDC Metadata
        # --------------------------------------------------

        F.col(
            "cdc.operation"
        ).alias("_operation"),

        F.col(
            "cdc.batch_id"
        ).alias("_batch_id"),

        F.col(
            "cdc.event_id"
        ).alias("_event_id"),

        F.col(
            "cdc.entity"
        ).alias("_entity"),
    )


# ==========================================================
# Silver Account SCD Type 2 Table
# ==========================================================


dp.create_streaming_table(

    name=SILVER_ACCOUNT,

    comment=(
        "Silver Account SCD Type 2 table"
    ),
)


# ==========================================================
# AUTO CDC
# ==========================================================


dp.create_auto_cdc_flow(

    target=SILVER_ACCOUNT,

    source="account_cdc_source",

    keys=[
        "account_id",
    ],

    sequence_by=F.col(
        "_batch_id"
    ),

    apply_as_deletes=(
        F.col("_operation")
        ==
        F.lit("delete")
    ),

    except_column_list=[
        "_operation",
        "_batch_id",
        "_event_id",
        "_entity",
    ],

    stored_as_scd_type=2,
)