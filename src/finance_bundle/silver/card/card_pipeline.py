"""
Card Silver Lakeflow SDP Pipeline.

Bronze Card
       +
Bronze Card CDC
       |
       v
card_cdc_source
       |
       v
Silver Card SCD2
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables


# ==========================================================
# Table Names
# ==========================================================

BRONZE_CARD = Catalog.bronze(
    Tables.CARD
)

BRONZE_CARD_CDC = Catalog.bronze(
    Tables.CARD_CDC
)

SILVER_CARD = Catalog.silver(
    Tables.CARD
)


# ==========================================================
# Bronze Card CDC Source
# ==========================================================

@dp.temporary_view(
    name="card_cdc_source",
)
def card_cdc_source():

    card_df = dp.read(
        BRONZE_CARD
    )

    cdc_df = dp.read_stream(
        BRONZE_CARD_CDC
    )

    # ------------------------------------------------------
    # Normalize Card columns
    # ------------------------------------------------------

    for column_name in card_df.columns:

        normalized = (
            column_name
            .strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
        )

        if normalized != column_name:

            card_df = card_df.withColumnRenamed(
                column_name,
                normalized,
            )

    # ------------------------------------------------------
    # Normalize CDC columns
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

    # ------------------------------------------------------
    # Normalize CDC values
    # ------------------------------------------------------

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
            "card_id",
            F.trim(
                F.col("card_id")
            ),
        )

        .withColumn(
            "customer_id",
            F.trim(
                F.col("customer_id")
            ),
        )

        .withColumn(
            "account_id",
            F.trim(
                F.col("account_id")
            ),
        )

        .withColumn(
            "old_status",
            F.upper(
                F.trim(
                    F.col("old_status")
                )
            ),
        )

        .withColumn(
            "new_status",
            F.upper(
                F.trim(
                    F.col("new_status")
                )
            ),
        )

        .withColumn(
            "card_type",
            F.upper(
                F.trim(
                    F.col("card_type")
                )
            ),
        )

        .withColumn(
            "network",
            F.upper(
                F.trim(
                    F.col("network")
                )
            ),
        )
    )

    # ------------------------------------------------------
    # Normalize Card values
    # ------------------------------------------------------

    string_columns = [
        "card_id",
        "account_id",
        "customer_id",
        "card_number",
        "card_type",
        "network",
        "cvv",
        "status",
    ]

    for column_name in string_columns:

        if column_name in card_df.columns:

            card_df = card_df.withColumn(
                column_name,
                F.trim(
                    F.col(column_name).cast("string")
                ),
            )

    # ------------------------------------------------------
    # Normalize Card status
    # ------------------------------------------------------

    if "status" in card_df.columns:

        card_df = card_df.withColumn(
            "status",
            F.upper(
                F.col("status")
            ),
        )

    # ------------------------------------------------------
    # Normalize Card numeric columns
    # ------------------------------------------------------

    if "credit_limit" in card_df.columns:

        card_df = card_df.withColumn(
            "credit_limit",
            F.col("credit_limit").cast("double"),
        )

    if "daily_limit" in card_df.columns:

        card_df = card_df.withColumn(
            "daily_limit",
            F.col("daily_limit").cast("double"),
        )

    # ------------------------------------------------------
    # Normalize Card dates
    # ------------------------------------------------------

    if "issue_date" in card_df.columns:

        card_df = card_df.withColumn(
            "issue_date",
            F.to_date(
                F.col("issue_date")
            ),
        )

    if "expiry_date" in card_df.columns:

        card_df = card_df.withColumn(
            "expiry_date",
            F.to_date(
                F.col("expiry_date")
            ),
        )

    # ------------------------------------------------------
    # Remove duplicate Card state
    # ------------------------------------------------------

    card_df = card_df.dropDuplicates(
        ["card_id"]
    )

    # ------------------------------------------------------
    # Join CDC events to current Card state
    # ------------------------------------------------------

    joined_df = (
        cdc_df.alias("cdc")
        .join(
            card_df.alias("card"),

            F.col("cdc.card_id")
            ==
            F.col("card.card_id"),

            "left",
        )
    )

    # ------------------------------------------------------
    # Build Card CDC after-image
    # ------------------------------------------------------

    return joined_df.select(

        F.col(
            "card.card_id"
        ).alias("card_id"),

        # CDC contains account_id
        F.coalesce(
            F.col("cdc.account_id"),
            F.col("card.account_id"),
        ).alias("account_id"),

        # CDC contains customer_id
        F.coalesce(
            F.col("cdc.customer_id"),
            F.col("card.customer_id"),
        ).alias("customer_id"),

        F.col(
            "card.card_number"
        ).alias("card_number"),

        # CDC contains card_type
        F.coalesce(
            F.col("cdc.card_type"),
            F.col("card.card_type"),
        ).alias("card_type"),

        # CDC contains network
        F.coalesce(
            F.col("cdc.network"),
            F.col("card.network"),
        ).alias("network"),

        F.col(
            "card.credit_limit"
        ).alias("credit_limit"),

        F.col(
            "card.daily_limit"
        ).alias("daily_limit"),

        F.col(
            "card.cvv"
        ).alias("cvv"),

        F.col(
            "card.issue_date"
        ).alias("issue_date"),

        F.col(
            "card.expiry_date"
        ).alias("expiry_date"),

        # Use new_status as the CDC after-image
        F.coalesce(
            F.col("cdc.new_status"),
            F.col("card.status"),
        ).alias("status"),

        # --------------------------------------------------
        # CDC metadata
        # --------------------------------------------------

        F.col(
            "cdc.operation"
        ).alias("_operation"),

        F.col(
            "cdc.change_timestamp"
        ).alias("_sequence_timestamp"),

        F.col(
            "cdc.event_timestamp"
        ).alias("_event_timestamp"),

        F.col(
            "cdc.event_id"
        ).alias("_event_id"),
    )


# ==========================================================
# Silver Card
# ==========================================================

dp.create_streaming_table(

    name=SILVER_CARD,

    comment=(
        "Silver Card SCD Type 2 table"
    ),
)


# ==========================================================
# AUTO CDC
# ==========================================================

dp.create_auto_cdc_flow(

    target=SILVER_CARD,

    source="card_cdc_source",

    keys=[
        "card_id",
    ],

    sequence_by=F.col(
        "_sequence_timestamp"
    ),

    apply_as_deletes=(
        F.col("_operation")
        ==
        F.lit("delete")
    ),

    except_column_list=[
        "_operation",
        "_sequence_timestamp",
        "_event_timestamp",
        "_event_id",
    ],

    stored_as_scd_type=2,
)