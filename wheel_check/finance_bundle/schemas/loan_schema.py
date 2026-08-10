from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    DateType,
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
    StructField("status", StringType(), True),
])


LOAN_SCHEMA_HINTS = """
loan_id string,
customer_id string,
branch_id string,
loan_type string,
loan_amount double,
interest_rate double,
tenure_years int,
monthly_emi double,
paid_emi int,
remaining_emi int,
outstanding_balance double,
loan_to_income_ratio double,
sanction_date date,
status string
"""