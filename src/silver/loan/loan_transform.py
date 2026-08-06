from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    current_timestamp,
    current_date,
    when,
    lit,
    to_date
)


def transform_loan(df: DataFrame) -> DataFrame:

    df = (

        df

        # Trim columns
        .withColumn("loan_id", trim(col("loan_id")))
        .withColumn("customer_id", trim(col("customer_id")))
        .withColumn("branch_id", trim(col("branch_id")))
        .withColumn("loan_type", trim(col("loan_type")))
        .withColumn("status", upper(trim(col("status"))))

        # Date conversion
        .withColumn(
            "sanction_date",
            to_date(col("sanction_date"))
        )

        # Numeric conversions
        .withColumn(
            "loan_amount",
            col("loan_amount").cast("double")
        )

        .withColumn(
            "interest_rate",
            col("interest_rate").cast("double")
        )

        .withColumn(
            "tenure_years",
            col("tenure_years").cast("int")
        )

        .withColumn(
            "monthly_emi",
            col("monthly_emi").cast("double")
        )

        .withColumn(
            "paid_emi",
            col("paid_emi").cast("int")
        )

        .withColumn(
            "remaining_emi",
            col("remaining_emi").cast("int")
        )

        .withColumn(
            "outstanding_balance",
            col("outstanding_balance").cast("double")
        )

        .withColumn(
            "loan_to_income_ratio",
            col("loan_to_income_ratio").cast("double")
        )

        # Normalize null values
        .fillna({

            "loan_type": "UNKNOWN",

            "status": "UNKNOWN",

            "interest_rate": 0,

            "loan_amount": 0,

            "monthly_emi": 0,

            "paid_emi": 0,

            "remaining_emi": 0,

            "outstanding_balance": 0,

            "loan_to_income_ratio": 0

        })

        # Standardize values
        .withColumn(

            "loan_type",

            when(col("loan_type") == "HOME", "HOME LOAN")
            .when(col("loan_type") == "PERSONAL", "PERSONAL LOAN")
            .otherwise(col("loan_type"))

        )

        .withColumn(

            "status",

            when(col("status") == "ACTIVE", "ACTIVE")
            .when(col("status") == "CLOSED", "CLOSED")
            .when(col("status") == "DEFAULTED", "DEFAULTED")
            .otherwise("UNKNOWN")

        )

        # Remove duplicates
        .dropDuplicates(["loan_id"])

        # Audit columns
        .withColumn(
            "silver_load_timestamp",
            current_timestamp()
        )

        .withColumn(
            "silver_load_date",
            current_date()
        )

    )

    return df