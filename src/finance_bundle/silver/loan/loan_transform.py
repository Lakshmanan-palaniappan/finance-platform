from pyspark.sql import DataFrame
from pyspark.sql import functions as F


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
# MASTER LOAN TRANSFORMATION
# ==========================================================

def transform_loan(df: DataFrame) -> DataFrame:

    for c in df.columns:
        normalized = (
            c.strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
        )

        if normalized != c:
            df = df.withColumnRenamed(c, normalized)

    string_columns = [
        "loan_id",
        "customer_id",
        "branch_id",
        "loan_type",
        "status",
    ]

    for c in string_columns:
        if c in df.columns:
            df = df.withColumn(
                c,
                F.trim(F.col(c).cast("string"))
            )

            df = df.withColumn(
                c,
                F.when(
                    F.col(c).isin(
                        "",
                        "NULL",
                        "null",
                        "N/A",
                        "NA"
                    ),
                    None
                ).otherwise(F.col(c))
            )

    if "loan_type" in df.columns:
        df = df.withColumn(
            "loan_type",
            F.upper(F.col("loan_type"))
        )

        df = df.withColumn(
            "loan_type",
            F.when(
                F.col("loan_type") == "HOME",
                "HOME LOAN"
            )
            .when(
                F.col("loan_type") == "PERSONAL",
                "PERSONAL LOAN"
            )
            .otherwise(F.col("loan_type"))
        )

    if "status" in df.columns:
        df = df.withColumn(
            "status",
            F.upper(F.col("status"))
        )

    # Numeric columns
    numeric_columns = {
        "loan_amount": "double",
        "interest_rate": "double",
        "tenure_years": "int",
        "monthly_emi": "double",
        "paid_emi": "int",
        "remaining_emi": "int",
        "outstanding_balance": "double",
        "loan_to_income_ratio": "double",
    }

    for c, dtype in numeric_columns.items():
        if c in df.columns:
            df = df.withColumn(
                c,
                F.col(c).cast(dtype)
            )

    if "sanction_date" in df.columns:
        df = df.withColumn(
            "sanction_date",
            F.to_date(F.col("sanction_date"))
        )

    return df


# ==========================================================
# PREPARE CDC
#
# INPUT:
#   1. Streaming CDC
#   2. Static Bronze Loan master
#
# OUTPUT:
#   Complete CDC after-image
# ==========================================================

def prepare_loan_cdc(
    cdc_df: DataFrame,
    loan_df: DataFrame
) -> DataFrame:

    # ------------------------------------------------------
    # Clean static master
    # ------------------------------------------------------

    master = (
        transform_loan(loan_df)
        .select(*LOAN_COLUMNS)
        .dropDuplicates(["loan_id"])
    )

    # ------------------------------------------------------
    # Normalize CDC column names
    # ------------------------------------------------------

    for c in cdc_df.columns:

        normalized = (
            c.strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
        )

        if normalized != c:
            cdc_df = cdc_df.withColumnRenamed(
                c,
                normalized
            )

    # ------------------------------------------------------
    # Normalize CDC fields
    # ------------------------------------------------------

    cdc_df = (
        cdc_df

        .withColumn(
            "entity",
            F.lower(F.trim(F.col("entity")))
        )

        .withColumn(
            "operation",
            F.lower(F.trim(F.col("operation")))
        )

        .withColumn(
            "loan_id",
            F.trim(F.col("loan_id"))
        )

        .withColumn(
            "customer_id",
            F.trim(F.col("customer_id"))
        )

        .withColumn(
            "batch_id",
            F.trim(F.col("batch_id"))
        )

        .withColumn(
            "event_id",
            F.trim(F.col("event_id"))
        )

        .withColumn(
            "new_balance",
            F.col("new_balance").cast("double")
        )

        .withColumn(
            "new_status",
            F.upper(F.trim(F.col("new_status")))
        )

        .withColumn(
            "event_timestamp",
            F.to_timestamp(F.col("event_timestamp"))
        )

        .withColumn(
            "change_timestamp",
            F.to_timestamp(F.col("change_timestamp"))
        )
    )

    # ------------------------------------------------------
    # STREAM + STATIC JOIN
    #
    # CDC = STREAM
    # MASTER = STATIC
    # ------------------------------------------------------

    joined = (
        cdc_df.alias("cdc")
        .join(
            master.alias("loan"),
            F.col("cdc.loan_id") ==
            F.col("loan.loan_id"),
            "left"
        )
    )

    # ------------------------------------------------------
    # COMPLETE AFTER IMAGE
    # ------------------------------------------------------

    return joined.select(

        F.col("cdc.loan_id").alias("loan_id"),

        F.coalesce(
            F.col("cdc.customer_id"),
            F.col("loan.customer_id")
        ).alias("customer_id"),

        F.col("loan.branch_id").alias("branch_id"),

        F.col("loan.loan_type").alias("loan_type"),

        F.col("loan.loan_amount").alias("loan_amount"),

        F.col("loan.interest_rate").alias("interest_rate"),

        F.col("loan.tenure_years").alias("tenure_years"),

        F.col("loan.monthly_emi").alias("monthly_emi"),

        F.col("loan.paid_emi").alias("paid_emi"),

        F.col("loan.remaining_emi").alias("remaining_emi"),

        F.coalesce(
            F.col("cdc.new_balance"),
            F.col("loan.outstanding_balance")
        ).alias("outstanding_balance"),

        F.col(
            "loan.loan_to_income_ratio"
        ).alias("loan_to_income_ratio"),

        F.col("loan.sanction_date").alias("sanction_date"),

        F.coalesce(
            F.col("cdc.new_status"),
            F.col("loan.status")
        ).alias("status"),

        # CDC metadata
        F.col("cdc.operation").alias("_operation"),

        F.col("cdc.batch_id").alias("_batch_id"),

        F.col("cdc.event_id").alias("_event_id"),

        F.col("cdc.entity").alias("_entity"),

        F.col(
            "cdc.event_timestamp"
        ).alias("_event_timestamp"),

        F.coalesce(
            F.col("cdc.change_timestamp"),
            F.col("cdc.event_timestamp")
        ).alias("_change_timestamp"),
    )