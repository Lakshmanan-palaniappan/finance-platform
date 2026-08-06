from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    DateType
)

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

    StructField("status", StringType(), True)
])