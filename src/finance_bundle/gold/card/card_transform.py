from pyspark import pipelines as dp
from pyspark.sql import functions as F

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables


# ==========================================================
# Read Silver Card
# ==========================================================

def read_silver_card():

    return dp.read(
        Catalog.silver(Tables.CARD)
    )


# ==========================================================
# Get Current Card Records
# ==========================================================

def get_current_cards(df):

    if "_is_current" in df.columns:

        return df.filter(
            F.col("_is_current") == F.lit(True)
        )

    if "is_current" in df.columns:

        return df.filter(
            F.col("is_current") == F.lit(True)
        )

    return df


# ==========================================================
# Apply Business Rules
# ==========================================================

def apply_business_rules(df):

    return (
        df

        .withColumn(
            "card_status",
            F.upper(
                F.trim(
                    F.col("status")
                )
            ),
        )

        .withColumn(
            "card_network",
            F.upper(
                F.trim(
                    F.col("network")
                )
            ),
        )

        .withColumn(
            "card_category",
            F.upper(
                F.trim(
                    F.col("card_type")
                )
            ),
        )

        .withColumn(
            "is_active",
            F.when(
                F.col("card_status") == "ACTIVE",
                1,
            ).otherwise(0),
        )

        .withColumn(
            "is_blocked",
            F.when(
                F.col("card_status") == "BLOCKED",
                1,
            ).otherwise(0),
        )

        .withColumn(
            "is_inactive",
            F.when(
                F.col("card_status") == "INACTIVE",
                1,
            ).otherwise(0),
        )

        .withColumn(
            "is_expired",
            F.when(
                F.col("expiry_date") < F.current_date(),
                1,
            ).otherwise(0),
        )
    )


# ==========================================================
# Create Card Gold Metrics
# ==========================================================

def create_card_metrics(df):

    return (
        df

        .groupBy(
            "card_category",
            "card_network",
            "card_status",
        )

        .agg(

            F.countDistinct(
                "card_id"
            ).alias(
                "total_cards"
            ),

            F.countDistinct(
                "customer_id"
            ).alias(
                "unique_customers"
            ),

            F.sum(
                "is_active"
            ).alias(
                "active_cards"
            ),

            F.sum(
                "is_blocked"
            ).alias(
                "blocked_cards"
            ),

            F.sum(
                "is_inactive"
            ).alias(
                "inactive_cards"
            ),

            F.sum(
                "is_expired"
            ).alias(
                "expired_cards"
            ),

            F.sum(
                "credit_limit"
            ).alias(
                "total_credit_limit"
            ),

            F.avg(
                "credit_limit"
            ).alias(
                "avg_credit_limit"
            ),

            F.max(
                "credit_limit"
            ).alias(
                "max_credit_limit"
            ),

            F.sum(
                "daily_limit"
            ).alias(
                "total_daily_limit"
            ),

            F.avg(
                "daily_limit"
            ).alias(
                "avg_daily_limit"
            ),

            F.max(
                "daily_limit"
            ).alias(
                "max_daily_limit"
            ),
        )
    )


# ==========================================================
# Final Gold Transformation
# ==========================================================

def transform_card_gold():

    silver_df = read_silver_card()

    current_df = get_current_cards(
        silver_df
    )

    business_df = apply_business_rules(
        current_df
    )

    gold_df = create_card_metrics(
        business_df
    )

    # ------------------------------------------------------
    # KPI calculations
    # ------------------------------------------------------

    gold_df = (
        gold_df

        .withColumn(
            "active_card_percentage",
            F.when(
                F.col("total_cards") > 0,

                (
                    F.col("active_cards")
                    /
                    F.col("total_cards")
                ) * 100,

            ).otherwise(0),
        )

        .withColumn(
            "blocked_card_percentage",
            F.when(
                F.col("total_cards") > 0,

                (
                    F.col("blocked_cards")
                    /
                    F.col("total_cards")
                ) * 100,

            ).otherwise(0),
        )

        .withColumn(
            "expired_card_percentage",
            F.when(
                F.col("total_cards") > 0,

                (
                    F.col("expired_cards")
                    /
                    F.col("total_cards")
                ) * 100,

            ).otherwise(0),
        )
    )

    return gold_df