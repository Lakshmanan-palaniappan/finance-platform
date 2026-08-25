from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ==========================================================
# CLEAN EXCHANGE RATE
# ==========================================================

def clean_exchange_rate(df: DataFrame) -> DataFrame:
    """
    Cleans and standardizes Bronze Exchange Rate data.

    Input:
        Bronze Exchange Rate DataFrame

    Transformations:
        - Trim whitespace
        - Normalize empty strings to null
        - Standardize currency codes
        - Cast exchange rate
        - Cast effective date
    """

    return (
        df

        # --------------------------------------------------
        # Trim whitespace
        # --------------------------------------------------

        .withColumn(
            "base_currency",
            F.trim(F.col("base_currency"))
        )

        .withColumn(
            "target_currency",
            F.trim(F.col("target_currency"))
        )

        # --------------------------------------------------
        # Normalize empty strings to NULL
        # --------------------------------------------------

        .withColumn(
            "base_currency",
            F.when(
                F.col("base_currency") == "",
                F.lit(None)
            ).otherwise(F.col("base_currency"))
        )

        .withColumn(
            "target_currency",
            F.when(
                F.col("target_currency") == "",
                F.lit(None)
            ).otherwise(F.col("target_currency"))
        )

        # --------------------------------------------------
        # Standardize currency codes
        # --------------------------------------------------

        .withColumn(
            "base_currency",
            F.upper(F.col("base_currency"))
        )

        .withColumn(
            "target_currency",
            F.upper(F.col("target_currency"))
        )

        # --------------------------------------------------
        # Cast data types
        # --------------------------------------------------

        .withColumn(
            "exchange_rate",
            F.col("exchange_rate").cast("decimal(18,6)")
        )

        .withColumn(
            "effective_date",
            F.to_date(F.col("effective_date"))
        )
    )


# ==========================================================
# VALIDATION FLAGS
# ==========================================================

def add_validation_flags(df: DataFrame) -> DataFrame:
    """
    Adds validation flags used to separate valid
    and invalid records.
    """

    return (
        df

        # --------------------------------------------------
        # Base currency validation
        # --------------------------------------------------

        .withColumn(
            "_invalid_base_currency",
            F.col("base_currency").isNull()
        )

        # --------------------------------------------------
        # Target currency validation
        # --------------------------------------------------

        .withColumn(
            "_invalid_target_currency",
            F.col("target_currency").isNull()
        )

        # --------------------------------------------------
        # Exchange rate validation
        # --------------------------------------------------

        .withColumn(
            "_invalid_exchange_rate",
            (
                F.col("exchange_rate").isNull()
                | (F.col("exchange_rate") <= 0)
            )
        )

        # --------------------------------------------------
        # Effective date validation
        # --------------------------------------------------

        .withColumn(
            "_invalid_effective_date",
            F.col("effective_date").isNull()
        )

        # --------------------------------------------------
        # Base and target currency should be different
        # --------------------------------------------------

        .withColumn(
            "_invalid_currency_pair",
            (
                F.col("base_currency").isNotNull()
                & F.col("target_currency").isNotNull()
                & (
                    F.col("base_currency")
                    == F.col("target_currency")
                )
            )
        )

        # --------------------------------------------------
        # Rescued data validation
        # --------------------------------------------------

        .withColumn(
            "_has_rescued_data",
            (
                F.col("_rescued_data").isNotNull()
                & (
                    F.trim(
                        F.col("_rescued_data")
                    ) != ""
                )
            )
        )

        # --------------------------------------------------
        # Overall validation
        # --------------------------------------------------

        .withColumn(
            "_is_invalid",
            (
                F.col("_invalid_base_currency")
                | F.col("_invalid_target_currency")
                | F.col("_invalid_exchange_rate")
                | F.col("_invalid_effective_date")
                | F.col("_invalid_currency_pair")
                | F.col("_has_rescued_data")
            )
        )
    )


# ==========================================================
# VALID RECORDS
# ==========================================================

def get_valid_exchange_rates(
    df: DataFrame,
) -> DataFrame:
    """
    Returns valid Exchange Rate records only.
    """

    return (
        df
        .filter(~F.col("_is_invalid"))
        .drop(
            "_invalid_base_currency",
            "_invalid_target_currency",
            "_invalid_exchange_rate",
            "_invalid_effective_date",
            "_invalid_currency_pair",
            "_has_rescued_data",
            "_is_invalid",
        )
    )


# ==========================================================
# QUARANTINE RECORDS
# ==========================================================

def get_quarantine_exchange_rates(
    df: DataFrame,
) -> DataFrame:
    """
    Returns invalid Exchange Rate records
    with a quarantine reason.
    """

    return (
        df
        .filter(F.col("_is_invalid"))

        .withColumn(
            "quarantine_reason",
            F.concat_ws(
                ", ",

                F.when(
                    F.col("_invalid_base_currency"),
                    F.lit("Missing base_currency")
                ),

                F.when(
                    F.col("_invalid_target_currency"),
                    F.lit("Missing target_currency")
                ),

                F.when(
                    F.col("_invalid_exchange_rate"),
                    F.lit(
                        "Invalid exchange_rate"
                    )
                ),

                F.when(
                    F.col("_invalid_effective_date"),
                    F.lit(
                        "Invalid effective_date"
                    )
                ),

                F.when(
                    F.col("_invalid_currency_pair"),
                    F.lit(
                        "base_currency equals target_currency"
                    )
                ),

                F.when(
                    F.col("_has_rescued_data"),
                    F.lit(
                        "Unexpected/rescued data"
                    )
                ),
            )
        )

        .withColumn(
            "quarantine_timestamp",
            F.current_timestamp()
        )

        .drop(
            "_invalid_base_currency",
            "_invalid_target_currency",
            "_invalid_exchange_rate",
            "_invalid_effective_date",
            "_invalid_currency_pair",
            "_has_rescued_data",
            "_is_invalid",
        )
    )


# ==========================================================
# DEDUPLICATION
# ==========================================================

def deduplicate_exchange_rates(
    df: DataFrame,
) -> DataFrame:
    """
    Removes duplicate Exchange Rate records.

    Business key:
        base_currency
        target_currency
        effective_date
    """

    return (
        df.dropDuplicates(
            [
                "base_currency",
                "target_currency",
                "effective_date",
            ]
        )
    )


# ==========================================================
# FINAL SILVER TRANSFORMATION
# ==========================================================

def transform_exchange_rate(
    df: DataFrame,
) -> DataFrame:
    """
    Complete Silver transformation.
    """

    cleaned_df = clean_exchange_rate(df)

    validated_df = add_validation_flags(
        cleaned_df
    )

    valid_df = get_valid_exchange_rates(
        validated_df
    )

    silver_df = deduplicate_exchange_rates(
        valid_df
    )

    return silver_df


# ==========================================================
# QUARANTINE TRANSFORMATION
# ==========================================================

def transform_exchange_rate_quarantine(
    df: DataFrame,
) -> DataFrame:
    """
    Complete quarantine transformation.
    """

    cleaned_df = clean_exchange_rate(df)

    validated_df = add_validation_flags(
        cleaned_df
    )

    quarantine_df = get_quarantine_exchange_rates(
        validated_df
    )

    return quarantine_df