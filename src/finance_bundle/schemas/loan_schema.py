from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    DateType,
)


# ==========================================================
# Explicit Spark schema
# ==========================================================

loan_schema = StructType([
    StructField("loan_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("branch_id", StringType(), False),

    StructField("loan_type", StringType(), True),
    StructField("loan_amount", DoubleType(), True),
    StructField("interest_rate", DoubleType(), True),
    StructField("tenure_years", IntegerType(), True),
    StructField("monthly_emi", DoubleType(), True),
    StructField("paid_emi", IntegerType(), True),
    StructField("remaining_emi", IntegerType(), True),
    StructField("outstanding_balance", DoubleType(), True),
    StructField("loan_to_income_ratio", DoubleType(), True),
    StructField("sanction_date", DateType(), True),
    StructField("status", StringType(), True),
])


# ==========================================================
# Auto Loader schema hints
# ==========================================================

LOAN_SCHEMA_HINTS = """
loan_id STRING,
customer_id STRING,
branch_id STRING,
loan_type STRING,
loan_amount DOUBLE,
interest_rate DOUBLE,
tenure_years INT,
monthly_emi DOUBLE,
paid_emi INT,
remaining_emi INT,
outstanding_balance DOUBLE,
loan_to_income_ratio DOUBLE,
sanction_date DATE,
status STRING
"""