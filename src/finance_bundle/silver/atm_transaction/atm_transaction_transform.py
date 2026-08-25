from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    when,
    lit,
    count,
    sum,
    row_number,
)
from pyspark.sql.window import Window


# ==========================================================
# Valid ATM transaction statuses
# ==========================================================

VALID_STATUSES = [
    "SUCCESS",
    "FAILED",
    "DECLINED",
]


# ==========================================================
# Clean ATM transaction
# ==========================================================

def clean_atm_transaction(
    df: DataFrame,
) -> DataFrame:

    string_columns = [
        "atm_transaction_id",
        "card_id",
        "account_id",
        "customer_id",
        "atm_id",
        "status",
    ]

    # ------------------------------------------------------
    # Trim whitespace
    # ------------------------------------------------------

    for column_name in string_columns:

        df = df.withColumn(
            column_name,
            trim(col(column_name)),
        )

    # ------------------------------------------------------
    # Convert blank strings to NULL
    # ------------------------------------------------------

    for column_name in string_columns:

        df = df.withColumn(
            column_name,
            when(
                col(column_name) == "",
                lit(None),
            ).otherwise(
                col(column_name)
            ),
        )

    # ------------------------------------------------------
    # Standardize status
    # ------------------------------------------------------

    df = df.withColumn(
        "status",
        upper(col("status")),
    )

    return df


# ==========================================================
# Validate ATM transaction
# ==========================================================

def validate_atm_transaction(
    df: DataFrame,
) -> DataFrame:

    return (
        df

        # --------------------------------------------------
        # Business key validation
        # --------------------------------------------------

        .withColumn(
            "_invalid_transaction_id",
            col("atm_transaction_id").isNull(),
        )

        # --------------------------------------------------
        # ATM ID validation
        # --------------------------------------------------

        .withColumn(
            "_invalid_atm_id",
            col("atm_id").isNull(),
        )

        # --------------------------------------------------
        # Card validation
        # --------------------------------------------------

        .withColumn(
            "_invalid_card_id",
            col("card_id").isNull(),
        )

        # --------------------------------------------------
        # Account validation
        # --------------------------------------------------

        .withColumn(
            "_invalid_account_id",
            col("account_id").isNull(),
        )

        # --------------------------------------------------
        # Customer validation
        # --------------------------------------------------

        .withColumn(
            "_invalid_customer_id",
            col("customer_id").isNull(),
        )

        # --------------------------------------------------
        # Amount validation
        # --------------------------------------------------

        .withColumn(
            "_invalid_amount",
            (
                col("withdrawal_amount").isNull()
                |
                (col("withdrawal_amount") < 0)
            ),
        )

        # --------------------------------------------------
        # Timestamp validation
        # --------------------------------------------------

        .withColumn(
            "_invalid_timestamp",
            col("transaction_timestamp").isNull(),
        )

        # --------------------------------------------------
        # Status validation
        # --------------------------------------------------

        .withColumn(
            "_invalid_status",
            (
                col("status").isNull()
                |
                (~col("status").isin(VALID_STATUSES))
            ),
        )
    )


# ==========================================================
# Add validation result
# ==========================================================

def add_validation_result(
    df: DataFrame,
) -> DataFrame:

    invalid_condition = (
        col("_invalid_transaction_id")
        |
        col("_invalid_atm_id")
        |
        col("_invalid_card_id")
        |
        col("_invalid_account_id")
        |
        col("_invalid_customer_id")
        |
        col("_invalid_amount")
        |
        col("_invalid_timestamp")
        |
        col("_invalid_status")
    )

    return (
        df

        .withColumn(
            "_is_valid",
            ~invalid_condition,
        )

        .withColumn(
            "_validation_error",
            when(
                col("_invalid_transaction_id"),
                lit("Missing atm_transaction_id"),
            )
            .when(
                col("_invalid_atm_id"),
                lit("Missing atm_id"),
            )
            .when(
                col("_invalid_card_id"),
                lit("Missing card_id"),
            )
            .when(
                col("_invalid_account_id"),
                lit("Missing account_id"),
            )
            .when(
                col("_invalid_customer_id"),
                lit("Missing customer_id"),
            )
            .when(
                col("_invalid_amount"),
                lit("Invalid withdrawal_amount"),
            )
            .when(
                col("_invalid_timestamp"),
                lit("Missing transaction_timestamp"),
            )
            .when(
                col("_invalid_status"),
                lit("Invalid status"),
            )
            .otherwise(
                lit(None)
            ),
        )
    )


# ==========================================================
# Deduplicate ATM transactions
# ==========================================================

def deduplicate_atm_transactions(
    df: DataFrame,
) -> DataFrame:

    window_spec = (
        Window
        .partitionBy(
            "atm_transaction_id"
        )
        .orderBy(
            col(
                "transaction_timestamp"
            ).desc()
        )
    )

    return (
        df

        .withColumn(
            "_row_number",
            row_number(),
        )

        .filter(
            col("_row_number") == 1
        )

        .drop(
            "_row_number"
        )
    )


# ==========================================================
# Add repeated failure flag
# ==========================================================

def add_repeated_failure_flag(
    df: DataFrame,
) -> DataFrame:

    failure_window = (
        Window
        .partitionBy("atm_id")
        .orderBy(
            col("transaction_timestamp")
        )
        .rowsBetween(
            Window.currentRow - 2,
            Window.currentRow,
        )
    )

    return (
        df

        .withColumn(
            "_recent_failure_count",
            sum(
                when(
                    col("status").isin(
                        "FAILED",
                        "DECLINED",
                    ),
                    lit(1),
                ).otherwise(
                    lit(0)
                )
            ).over(
                failure_window
            ),
        )

        .withColumn(
            "repeated_failure_flag",
            col("_recent_failure_count") >= 3,
        )

        .drop(
            "_recent_failure_count"
        )
    )


# ==========================================================
# Add ATM usage metrics
# ==========================================================

def add_atm_usage_metrics(
    df: DataFrame,
) -> DataFrame:

    usage_window = (
        Window
        .partitionBy("atm_id")
    )

    return (
        df

        .withColumn(
            "atm_transaction_count",
            count("*").over(
                usage_window
            ),
        )

        .withColumn(
            "atm_total_withdrawal_amount",
            sum(
                "withdrawal_amount"
            ).over(
                usage_window
            ),
        )
    )


# ==========================================================
# Remove validation helper columns
# ==========================================================

def remove_validation_columns(
    df: DataFrame,
) -> DataFrame:

    return df.drop(
        "_invalid_transaction_id",
        "_invalid_atm_id",
        "_invalid_card_id",
        "_invalid_account_id",
        "_invalid_customer_id",
        "_invalid_amount",
        "_invalid_timestamp",
        "_invalid_status",
        "_is_valid",
        "_validation_error",
    )


# ==========================================================
# Final Silver transformation
# ==========================================================

def transform_silver_atm_transaction(
    df: DataFrame,
) -> DataFrame:

    df = clean_atm_transaction(df)

    df = validate_atm_transaction(df)

    df = add_validation_result(df)

    df = (
        df
        .filter(
            col("_is_valid") == True
        )
        .drop(
            "_invalid_transaction_id",
            "_invalid_atm_id",
            "_invalid_card_id",
            "_invalid_account_id",
            "_invalid_customer_id",
            "_invalid_amount",
            "_invalid_timestamp",
            "_invalid_status",
            "_is_valid",
            "_validation_error",
        )
    )

    return df