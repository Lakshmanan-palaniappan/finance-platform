from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ==========================================================
# LOAN COLUMNS
# ==========================================================

LOAN_COLUMNS = [
    "loan_id",
    "customer_id",
    "branch_id",
    "loan_type",
    "loan_amount",
    "interest_rate",
    "tenure_years",
    "monthly_emi",
    "paid_emi",
    "remaining_emi",
    "outstanding_balance",
    "loan_to_income_ratio",
    "sanction_date",
    "status",
]


# ==========================================================
# TRANSFORM NORMAL LOAN
# ==========================================================

def transform_loan(df: DataFrame) -> DataFrame:

    string_columns = [
        "loan_id",
        "customer_id",
        "branch_id",
        "loan_type",
        "status",
    ]

    # ------------------------------------------------------
    # Trim + normalize empty strings
    # ------------------------------------------------------

    for column_name in string_columns:

        df = df.withColumn(
            column_name,
            F.trim(F.col(column_name))
        )

        df = df.withColumn(
            column_name,
            F.when(
                F.col(column_name) == "",
                F.lit(None)
            ).otherwise(
                F.col(column_name)
            )
        )

    # ------------------------------------------------------
    # Standardize values
    # ------------------------------------------------------

    df = (
        df
        .withColumn(
            "loan_type",
            F.upper(F.col("loan_type"))
        )
        .withColumn(
            "status",
            F.upper(F.col("status"))
        )
    )

    # ------------------------------------------------------
    # Standardize loan types
    # ------------------------------------------------------

    df = df.withColumn(
        "loan_type",
        F.when(
            F.col("loan_type") == "HOME",
            F.lit("HOME LOAN")
        )
        .when(
            F.col("loan_type") == "PERSONAL",
            F.lit("PERSONAL LOAN")
        )
        .otherwise(
            F.col("loan_type")
        )
    )

    # ------------------------------------------------------
    # Standardize status
    # ------------------------------------------------------

    df = df.withColumn(
        "status",
        F.when(
            F.col("status").isin(
                "ACTIVE",
                "CLOSED",
                "DEFAULTED"
            ),
            F.col("status")
        )
        .otherwise(
            F.lit("UNKNOWN")
        )
    )

    # ------------------------------------------------------
    # Cast numeric/date fields
    # ------------------------------------------------------

    df = (
        df
        .withColumn(
            "loan_amount",
            F.col("loan_amount").cast("double")
        )
        .withColumn(
            "interest_rate",
            F.col("interest_rate").cast("double")
        )
        .withColumn(
            "tenure_years",
            F.col("tenure_years").cast("int")
        )
        .withColumn(
            "monthly_emi",
            F.col("monthly_emi").cast("double")
        )
        .withColumn(
            "paid_emi",
            F.col("paid_emi").cast("int")
        )
        .withColumn(
            "remaining_emi",
            F.col("remaining_emi").cast("int")
        )
        .withColumn(
            "outstanding_balance",
            F.col("outstanding_balance").cast("double")
        )
        .withColumn(
            "loan_to_income_ratio",
            F.col("loan_to_income_ratio").cast("double")
        )
        .withColumn(
            "sanction_date",
            F.to_date(F.col("sanction_date"))
        )
    )

    # ------------------------------------------------------
    # Normalize nullable fields
    # ------------------------------------------------------

    df = df.fillna(
        {
            "loan_type": "UNKNOWN",
            "status": "UNKNOWN",
            "loan_amount": 0.0,
            "interest_rate": 0.0,
            "monthly_emi": 0.0,
            "paid_emi": 0,
            "remaining_emi": 0,
            "outstanding_balance": 0.0,
            "loan_to_income_ratio": 0.0,
        }
    )

    return df


# ==========================================================
# VALIDATION
# ==========================================================

def add_loan_validation(df: DataFrame) -> DataFrame:

    return (
        df

        .withColumn(
            "_validation_error",

            F.when(
                F.col("loan_id").isNull(),
                F.lit("loan_id is NULL")
            )

            .when(
                F.col("customer_id").isNull(),
                F.lit("customer_id is NULL")
            )

            .when(
                F.col("branch_id").isNull(),
                F.lit("branch_id is NULL")
            )

            .when(
                F.col("loan_amount") < 0,
                F.lit("loan_amount is negative")
            )

            .when(
                F.col("interest_rate") < 0,
                F.lit("interest_rate is negative")
            )

            .when(
                F.col("tenure_years") <= 0,
                F.lit(
                    "tenure_years must be greater than zero"
                )
            )

            .when(
                F.col("outstanding_balance") < 0,
                F.lit(
                    "outstanding_balance is negative"
                )
            )

            .when(
                F.col("outstanding_balance")
                > F.col("loan_amount"),
                F.lit(
                    "outstanding_balance exceeds loan_amount"
                )
            )

            .otherwise(
                F.lit(None)
            )
        )

        .withColumn(
            "_is_valid",
            F.col("_validation_error").isNull()
        )
    )


# ==========================================================
# VALID RECORDS
# ==========================================================

def get_valid_records(df: DataFrame) -> DataFrame:

    return (
        df
        .filter(F.col("_is_valid"))
        .drop(
            "_validation_error",
            "_is_valid"
        )
    )


# ==========================================================
# QUARANTINE RECORDS
# ==========================================================

def get_quarantine_records(df: DataFrame) -> DataFrame:

    return (
        df
        .filter(~F.col("_is_valid"))
    )


# ==========================================================
# PREPARE CDC
# ==========================================================

def prepare_loan_cdc(
    cdc_df: DataFrame,
    loan_df: DataFrame
) -> DataFrame:

    # ------------------------------------------------------
    # Clean master Loan
    # ------------------------------------------------------

    loan_df = (
        transform_loan(loan_df)
        .select(*LOAN_COLUMNS)
    )

    # ------------------------------------------------------
    # Clean CDC
    # ------------------------------------------------------

    cdc_df = (
        cdc_df

        .withColumn(
            "operation",
            F.lower(
                F.trim(
                    F.col("operation")
                )
            )
        )

        .withColumn(
            "loan_id",
            F.trim(
                F.col("loan_id")
            )
        )

        .withColumn(
            "customer_id",
            F.trim(
                F.col("customer_id")
            )
        )

        .withColumn(
            "event_timestamp",
            F.to_timestamp(
                F.col("event_timestamp")
            )
        )

        .withColumn(
            "change_timestamp",
            F.to_timestamp(
                F.col("change_timestamp")
            )
        )
    )

    # ------------------------------------------------------
    # Join CDC with current Loan master
    # ------------------------------------------------------

    joined_df = (
        cdc_df.alias("cdc")
        .join(
            loan_df.alias("loan"),
            F.col("cdc.loan_id")
            == F.col("loan.loan_id"),
            "left"
        )
    )

    # ------------------------------------------------------
    # Build complete CDC record
    # ------------------------------------------------------

    result_df = joined_df.select(

        F.col("cdc.loan_id").alias(
            "loan_id"
        ),

        F.coalesce(
            F.col("cdc.customer_id"),
            F.col("loan.customer_id")
        ).alias(
            "customer_id"
        ),

        F.col("loan.branch_id").alias(
            "branch_id"
        ),

        F.col("loan.loan_type").alias(
            "loan_type"
        ),

        F.col("loan.loan_amount").alias(
            "loan_amount"
        ),

        F.col("loan.interest_rate").alias(
            "interest_rate"
        ),

        F.col("loan.tenure_years").alias(
            "tenure_years"
        ),

        F.col("loan.monthly_emi").alias(
            "monthly_emi"
        ),

        F.col("loan.paid_emi").alias(
            "paid_emi"
        ),

        F.col("loan.remaining_emi").alias(
            "remaining_emi"
        ),

        # CDC new balance overrides master value
        F.coalesce(
            F.col("cdc.new_balance").cast("double"),
            F.col("loan.outstanding_balance")
        ).alias(
            "outstanding_balance"
        ),

        F.col(
            "loan.loan_to_income_ratio"
        ).alias(
            "loan_to_income_ratio"
        ),

        F.col(
            "loan.sanction_date"
        ).alias(
            "sanction_date"
        ),

        # CDC new status overrides master value
        F.coalesce(
            F.upper(
                F.trim(
                    F.col("cdc.new_status")
                )
            ),
            F.col("loan.status")
        ).alias(
            "status"
        ),

        # --------------------------------------------------
        # CDC metadata
        # --------------------------------------------------

        F.col("cdc.operation").alias(
            "operation"
        ),

        F.col("cdc.event_id").alias(
            "event_id"
        ),

        F.col("cdc.batch_id").alias(
            "batch_id"
        ),

        F.col("cdc.source_system").alias(
            "source_system"
        ),

        F.col("cdc.event_timestamp").alias(
            "event_timestamp"
        ),

        F.coalesce(
            F.col("cdc.change_timestamp"),
            F.col("cdc.event_timestamp"),
            F.current_timestamp()
        ).alias(
            "change_timestamp"
        )
    )

    return result_df