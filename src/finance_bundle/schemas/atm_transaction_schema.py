from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
    TimestampType,
)


# ==========================================================
# Explicit Spark schema
# ==========================================================

atm_transaction_schema = StructType([
    StructField("atm_transaction_id", StringType(), False),
    StructField("card_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("atm_id", StringType(), False),

    StructField("withdrawal_amount", DoubleType(), True),
    StructField("available_balance", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("fraud_flag", BooleanType(), True),
    StructField("transaction_timestamp", TimestampType(), True),
])


# ==========================================================
# Auto Loader schema hints
# ==========================================================

ATM_TRANSACTION_SCHEMA_HINTS = """
atm_transaction_id STRING,
card_id STRING,
account_id STRING,
customer_id STRING,
atm_id STRING,
withdrawal_amount DOUBLE,
available_balance DOUBLE,
status STRING,
fraud_flag BOOLEAN,
transaction_timestamp TIMESTAMP
"""