from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables


BRONZE_CARD = Catalog.bronze(Tables.CARD)
BRONZE_CARD_CDC = Catalog.bronze(Tables.CARD_CDC)
SILVER_CARD = Catalog.silver(Tables.CARD)


@dp.table(
    name=SILVER_CARD,
    comment="""
    Silver Card current-state table.
    Contains all Bronze cards with the latest CDC
    updates applied to matching card IDs.
    """
)
def silver_card():

    # ------------------------------------------------------
    # Bronze = complete original 100 cards
    # ------------------------------------------------------

    bronze = dp.read(BRONZE_CARD)

    # ------------------------------------------------------
    # CDC = 18 updates
    # ------------------------------------------------------

    cdc = dp.read(BRONZE_CARD_CDC)

    # ------------------------------------------------------
    # Get latest CDC event for each card
    # ------------------------------------------------------

    w = (
        Window
        .partitionBy("card_id")
        .orderBy(
            F.col("change_timestamp").desc(),
            F.col("event_timestamp").desc()
        )
    )

    cdc = (
        cdc
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # ------------------------------------------------------
    # Join CDC onto Bronze
    # ------------------------------------------------------

    joined = (
        bronze.alias("b")
        .join(
            cdc.alias("c"),
            F.col("b.card_id") == F.col("c.card_id"),
            "left"
        )
    )

    # ------------------------------------------------------
    # Keep ALL 100 Bronze cards.
    #
    # CDC overrides only the fields it actually contains.
    # ------------------------------------------------------

    result = joined.select(

        F.col("b.card_id").alias("card_id"),

        F.coalesce(
            F.col("c.customer_id"),
            F.col("b.customer_id")
        ).alias("customer_id"),

        F.coalesce(
            F.col("c.account_id"),
            F.col("b.account_id")
        ).alias("account_id"),

        F.col("b.card_number").alias("card_number"),

        F.coalesce(
            F.col("c.card_type"),
            F.col("b.card_type")
        ).alias("card_type"),

        F.coalesce(
            F.col("c.network"),
            F.col("b.network")
        ).alias("network"),

        F.col("b.credit_limit").alias("credit_limit"),

        F.col("b.daily_limit").alias("daily_limit"),

        F.col("b.cvv").alias("cvv"),

        F.col("b.issue_date").alias("issue_date"),

        F.col("b.expiry_date").alias("expiry_date"),

        F.coalesce(
            F.col("c.new_status"),
            F.col("b.status")
        ).alias("status")
    )

    # ------------------------------------------------------
    # Standardize
    # ------------------------------------------------------

    result = (
        result

        .withColumn(
            "card_id",
            F.trim(F.col("card_id").cast("string"))
        )

        .withColumn(
            "customer_id",
            F.trim(F.col("customer_id").cast("string"))
        )

        .withColumn(
            "account_id",
            F.trim(F.col("account_id").cast("string"))
        )

        .withColumn(
            "card_number",
            F.trim(F.col("card_number").cast("string"))
        )

        .withColumn(
            "card_type",
            F.upper(F.trim(F.col("card_type").cast("string")))
        )

        .withColumn(
            "network",
            F.upper(F.trim(F.col("network").cast("string")))
        )

        .withColumn(
            "status",
            F.upper(F.trim(F.col("status").cast("string")))
        )

        .withColumn(
            "credit_limit",
            F.col("credit_limit").cast("double")
        )

        .withColumn(
            "daily_limit",
            F.col("daily_limit").cast("double")
        )

        .withColumn(
            "issue_date",
            F.to_date(F.col("issue_date"))
        )

        .withColumn(
            "expiry_date",
            F.to_date(F.col("expiry_date"))
        )
    )

    return result