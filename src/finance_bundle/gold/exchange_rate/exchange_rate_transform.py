from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ==========================================================
# GOLD EXCHANGE RATE TRANSFORMATION
# ==========================================================

def create_exchange_rate_summary(
    df: DataFrame,
) -> DataFrame:
    """
    Creates Gold Exchange Rate Summary.

    Source:
        Silver Exchange Rate

    Grain:
        One record per base_currency + target_currency

    KPIs:
        - Latest exchange rate
        - Average exchange rate
        - Minimum exchange rate
        - Maximum exchange rate
        - Exchange rate volatility
        - Rate range
        - Observation count
        - Latest rate change
        - Latest rate change percentage
        - Rate direction
        - Business rule status
    """

    # ======================================================
    # 1. Select required Silver columns
    # ======================================================

    silver_df = df.select(
        "base_currency",
        "target_currency",
        "exchange_rate",
        "effective_date",
    )

    # ======================================================
    # 2. Calculate previous exchange rate
    # ======================================================

    rate_window = (
        Window
        .partitionBy(
            "base_currency",
            "target_currency",
        )
        .orderBy("effective_date")
    )

    rate_df = silver_df.withColumn(
        "previous_exchange_rate",
        F.lag("exchange_rate").over(rate_window),
    )

    # ======================================================
    # 3. Calculate rate change
    # ======================================================

    rate_df = rate_df.withColumn(
        "rate_change",
        F.when(
            F.col("previous_exchange_rate").isNotNull(),
            F.col("exchange_rate")
            - F.col("previous_exchange_rate"),
        ).otherwise(F.lit(0)),
    )

    rate_df = rate_df.withColumn(
        "rate_change_percentage",
        F.when(
            F.col("previous_exchange_rate").isNotNull()
            & (
                F.col("previous_exchange_rate") != 0
            ),
            (
                (
                    F.col("exchange_rate")
                    - F.col("previous_exchange_rate")
                )
                / F.col("previous_exchange_rate")
            )
            * 100,
        ).otherwise(F.lit(0)),
    )

    # ======================================================
    # 4. Aggregate exchange rate metrics
    # ======================================================

    summary_df = (
        rate_df
        .groupBy(
            "base_currency",
            "target_currency",
        )
        .agg(

            # ------------------------------------------------
            # Latest exchange rate
            # ------------------------------------------------

            F.max_by(
                "exchange_rate",
                "effective_date",
            ).alias(
                "latest_exchange_rate"
            ),

            # ------------------------------------------------
            # Average exchange rate
            # ------------------------------------------------

            F.round(
                F.avg("exchange_rate"),
                6,
            ).alias(
                "average_exchange_rate"
            ),

            # ------------------------------------------------
            # Minimum exchange rate
            # ------------------------------------------------

            F.min(
                "exchange_rate"
            ).alias(
                "minimum_exchange_rate"
            ),

            # ------------------------------------------------
            # Maximum exchange rate
            # ------------------------------------------------

            F.max(
                "exchange_rate"
            ).alias(
                "maximum_exchange_rate"
            ),

            # ------------------------------------------------
            # Volatility
            # ------------------------------------------------

            F.round(
                F.stddev("exchange_rate"),
                6,
            ).alias(
                "exchange_rate_volatility"
            ),

            # ------------------------------------------------
            # Number of observations
            # ------------------------------------------------

            F.count("*").alias(
                "observation_count"
            ),

            # ------------------------------------------------
            # First effective date
            # ------------------------------------------------

            F.min(
                "effective_date"
            ).alias(
                "first_effective_date"
            ),

            # ------------------------------------------------
            # Latest effective date
            # ------------------------------------------------

            F.max(
                "effective_date"
            ).alias(
                "latest_effective_date"
            ),

            # ------------------------------------------------
            # Latest rate change
            # ------------------------------------------------

            F.max_by(
                "rate_change",
                "effective_date",
            ).alias(
                "latest_rate_change"
            ),

            # ------------------------------------------------
            # Latest percentage change
            # ------------------------------------------------

            F.max_by(
                "rate_change_percentage",
                "effective_date",
            ).alias(
                "latest_rate_change_percentage"
            ),
        )
    )

    # ======================================================
    # 5. Rate range KPI
    # ======================================================

    summary_df = summary_df.withColumn(
        "rate_range",
        F.round(
            F.col("maximum_exchange_rate")
            - F.col("minimum_exchange_rate"),
            6,
        ),
    )

    # ======================================================
    # 6. Rate direction KPI
    # ======================================================

    summary_df = summary_df.withColumn(
        "rate_change_direction",
        F.when(
            F.col("latest_rate_change") > 0,
            F.lit("INCREASE"),
        )
        .when(
            F.col("latest_rate_change") < 0,
            F.lit("DECREASE"),
        )
        .otherwise(
            F.lit("NO_CHANGE")
        ),
    )

    # ======================================================
    # 7. Business rule validation
    # ======================================================

    summary_df = summary_df.withColumn(
        "business_rule_status",
        F.when(
            (
                F.col("latest_exchange_rate") > 0
            )
            & (
                F.col("minimum_exchange_rate") > 0
            )
            & (
                F.col("maximum_exchange_rate")
                >= F.col("minimum_exchange_rate")
            )
            & (
                F.col("base_currency")
                != F.col("target_currency")
            ),
            F.lit("VALID"),
        ).otherwise(
            F.lit("INVALID")
        ),
    )

    # ======================================================
    # 8. Final Gold columns
    # ======================================================

    return summary_df.select(
        "base_currency",
        "target_currency",
        "latest_exchange_rate",
        "average_exchange_rate",
        "minimum_exchange_rate",
        "maximum_exchange_rate",
        "exchange_rate_volatility",
        "rate_range",
        "observation_count",
        "first_effective_date",
        "latest_effective_date",
        "latest_rate_change",
        "latest_rate_change_percentage",
        "rate_change_direction",
        "business_rule_status",
    )