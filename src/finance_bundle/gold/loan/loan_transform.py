from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ==========================================================
# CURRENT LOAN RECORDS
# ==========================================================

def current_loans(
    df: DataFrame,
) -> DataFrame:

    return (
        df
        .filter(
            F.col(
                "__END_AT"
            ).isNull()
        )
    )


# ==========================================================
# BUSINESS METRICS
# ==========================================================

def add_loan_metrics(
    df: DataFrame,
) -> DataFrame:

    return (
        df

        .withColumn(
            "loan_outstanding_pct",

            F.when(
                F.col("loan_amount") > 0,

                (
                    F.col(
                        "outstanding_balance"
                    )
                    /
                    F.col(
                        "loan_amount"
                    )
                ) * 100,

            ).otherwise(
                F.lit(0.0)
            ),
        )

        .withColumn(
            "emi_completion_pct",

            F.when(
                (
                    F.col("paid_emi")
                    +
                    F.col("remaining_emi")
                ) > 0,

                (
                    F.col("paid_emi")
                    /
                    (
                        F.col("paid_emi")
                        +
                        F.col("remaining_emi")
                    )
                ) * 100,

            ).otherwise(
                F.lit(0.0)
            ),
        )

        .withColumn(
            "loan_age_days",

            F.datediff(
                F.current_date(),
                F.col("sanction_date"),
            ),
        )

        .withColumn(
            "risk_category",

            F.when(
                F.col("status") == "DEFAULTED",
                "HIGH",
            )

            .when(
                F.col("loan_to_income_ratio") >= 5,
                "HIGH",
            )

            .when(
                F.col("loan_to_income_ratio") >= 3,
                "MEDIUM",
            )

            .otherwise(
                "LOW"
            ),
        )
    )


# ==========================================================
# KPIs
# ==========================================================

def add_loan_kpis(
    df: DataFrame,
) -> DataFrame:

    return (
        df

        .withColumn(
            "outstanding_ratio_pct",

            F.when(
                F.col("loan_amount") > 0,

                (
                    F.col(
                        "outstanding_balance"
                    )
                    /
                    F.col(
                        "loan_amount"
                    )
                ) * 100,

            ).otherwise(
                F.lit(0.0)
            ),
        )

        .withColumn(
            "loan_performance",

            F.when(
                F.col("status") == "DEFAULTED",
                "CRITICAL",
            )

            .when(
                F.col("emi_completion_pct") >= 75,
                "GOOD",
            )

            .when(
                F.col("emi_completion_pct") >= 40,
                "WATCH",
            )

            .otherwise(
                "AT_RISK"
            ),
        )
    )


# ==========================================================
# BUSINESS VALIDATION
# ==========================================================

def add_business_validation(
    df: DataFrame,
) -> DataFrame:

    return (
        df

        .withColumn(
            "_business_rule_error",

            F.when(
                F.col("loan_amount") < 0,
                "Loan amount cannot be negative",
            )

            .when(
                F.col("outstanding_balance") < 0,
                "Outstanding balance cannot be negative",
            )

            .when(
                F.col("outstanding_balance")
                >
                F.col("loan_amount"),

                "Outstanding balance exceeds loan amount",
            )

            .when(
                F.col("interest_rate") < 0,
                "Interest rate cannot be negative",
            )

            .when(
                F.col("tenure_years") <= 0,
                "Tenure must be greater than zero",
            )

            .when(
                F.col("paid_emi") < 0,
                "Paid EMI cannot be negative",
            )

            .when(
                F.col("remaining_emi") < 0,
                "Remaining EMI cannot be negative",
            )

            .otherwise(
                F.lit(None)
            ),
        )

        .withColumn(
            "_business_rule_valid",
            F.col(
                "_business_rule_error"
            ).isNull(),
        )
    )


# ==========================================================
# FINAL GOLD TRANSFORMATION
# ==========================================================

def transform_loan_gold(
    silver_df: DataFrame,
) -> DataFrame:

    df = current_loans(
        silver_df
    )

    df = add_loan_metrics(
        df
    )

    df = add_loan_kpis(
        df
    )

    df = add_business_validation(
        df
    )

    return (
        df

        .filter(
            F.col(
                "_business_rule_valid"
            )
        )

        .drop(
            "_business_rule_error",
            "_business_rule_valid",
        )
    )