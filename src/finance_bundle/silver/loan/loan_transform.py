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
# NORMAL LOAN TRANSFORMATION
# ==========================================================

def transform_loan(df: DataFrame) -> DataFrame:

    string_columns = [
        "loan_id",
        "customer_id",
        "branch_id",
        "loan_type",
        "status",
    ]

    for column_name in string_columns:

        df = df.withColumn(
            column_name,
            F.trim(
                F.col(column_name)
            ),
        )

        df = df.withColumn(
            column_name,
            F.when(
                F.col(column_name) == "",
                F.lit(None),
            ).otherwise(
                F.col(column_name)
            ),
        )

    # ======================================================
    # STANDARDIZE
    # ======================================================

    df = (
        df
        .withColumn(
            "loan_type",
            F.upper(
                F.col("loan_type")
            ),
        )
        .withColumn(
            "status",
            F.upper(
                F.col("status")
            ),
        )
    )

    df = df.withColumn(
        "loan_type",
        F.when(
            F.col("loan_type") == "HOME",
            "HOME LOAN",
        )
        .when(
            F.col("loan_type") == "PERSONAL",
            "PERSONAL LOAN",
        )
        .otherwise(
            F.col("loan_type")
        ),
    )

    df = df.withColumn(
        "status",
        F.when(
            F.col("status").isin(
                "ACTIVE",
                "CLOSED",
                "DEFAULTED",
            ),
            F.col("status"),
        )
        .otherwise(
            "UNKNOWN"
        ),
    )

    # ======================================================
    # CAST
    # ======================================================

    df = (
        df
        .withColumn(
            "loan_amount",
            F.col("loan_amount").cast("double"),
        )
        .withColumn(
            "interest_rate",
            F.col("interest_rate").cast("double"),
        )
        .withColumn(
            "tenure_years",
            F.col("tenure_years").cast("int"),
        )
        .withColumn(
            "monthly_emi",
            F.col("monthly_emi").cast("double"),
        )
        .withColumn(
            "paid_emi",
            F.col("paid_emi").cast("int"),
        )
        .withColumn(
            "remaining_emi",
            F.col("remaining_emi").cast("int"),
        )
        .withColumn(
            "outstanding_balance",
            F.col("outstanding_balance").cast("double"),
        )
        .withColumn(
            "loan_to_income_ratio",
            F.col("loan_to_income_ratio").cast("double"),
        )
        .withColumn(
            "sanction_date",
            F.to_date(
                F.col("sanction_date")
            ),
        )
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
                "loan_id is NULL",
            )

            .when(
                F.col("customer_id").isNull(),
                "customer_id is NULL",
            )

            .when(
                F.col("branch_id").isNull(),
                "branch_id is NULL",
            )

            .when(
                F.col("loan_amount") < 0,
                "loan_amount is negative",
            )

            .when(
                F.col("interest_rate") < 0,
                "interest_rate is negative",
            )

            .when(
                F.col("tenure_years") <= 0,
                "tenure_years must be greater than zero",
            )

            .when(
                F.col("outstanding_balance") < 0,
                "outstanding_balance is negative",
            )

            .when(
                F.col("outstanding_balance")
                > F.col("loan_amount"),
                "outstanding_balance exceeds loan amount",
            )

            .otherwise(
                F.lit(None)
            )
        )

        .withColumn(
            "_is_valid",
            F.col("_validation_error").isNull(),
        )
    )


# ==========================================================
# VALID RECORDS
# ==========================================================

def get_valid_records(df: DataFrame) -> DataFrame:

    return (
        df
        .filter(
            F.col("_is_valid")
        )
        .drop(
            "_validation_error",
            "_is_valid",
        )
    )


# ==========================================================
# QUARANTINE
# ==========================================================

def get_quarantine_records(df: DataFrame) -> DataFrame:

    return (
        df
        .filter(
            ~F.col("_is_valid")
        )
    )


# ==========================================================
# PREPARE CDC
# ==========================================================

def prepare_loan_cdc(
    cdc_df: DataFrame,
    loan_df: DataFrame,
) -> DataFrame:

    # ------------------------------------------------------
    # Clean master loan
    # ------------------------------------------------------

    loan_df = (
        transform_loan(
            loan_df
        )
        .select(
            *LOAN_COLUMNS
        )
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
            ),
        )

        .withColumn(
            "loan_id",
            F.trim(
                F.col("loan_id")
            ),
        )

        .withColumn(
            "customer_id",
            F.trim(
                F.col("customer_id")
            ),
        )

        .withColumn(
            "event_timestamp",
            F.to_timestamp(
                F.col("event_timestamp")
            ),
        )

        .withColumn(
            "change_timestamp",
            F.to_timestamp(
                F.col("change_timestamp")
            ),
        )
    )

    # ------------------------------------------------------
    # JOIN CDC + MASTER
    # ------------------------------------------------------

    joined = (
        cdc_df.alias("cdc")
        .join(
            loan_df.alias("loan"),
            F.col("cdc.loan_id")
            == F.col("loan.loan_id"),
            "left",
        )
    )

    # ------------------------------------------------------
    # COMPLETE CDC RECORD
    # ------------------------------------------------------

    return joined.select(

        F.col("cdc.loan_id").alias(
            "loan_id"
        ),

        F.coalesce(
            F.col("cdc.customer_id"),
            F.col("loan.customer_id"),
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

        F.coalesce(
            F.col("cdc.new_balance").cast("double"),
            F.col("loan.outstanding_balance"),
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

        F.coalesce(
            F.upper(
                F.trim(
                    F.col("cdc.new_status")
                )
            ),
            F.col("loan.status"),
        ).alias(
            "status"
        ),

        # ==================================================
        # CDC METADATA
        # ==================================================

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
            F.current_timestamp(),
        ).alias(
            "change_timestamp"
        ),
    )