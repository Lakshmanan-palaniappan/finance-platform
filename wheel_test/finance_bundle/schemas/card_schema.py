from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    DateType,
)

# ==========================================================
# Explicit Spark schema
# ==========================================================

card_schema = StructType([
    StructField("card_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("customer_id", StringType(), False),

    StructField("card_number", StringType(), True),
    StructField("card_type", StringType(), True),
    StructField("network", StringType(), True),

    StructField("credit_limit", DoubleType(), True),
    StructField("daily_limit", DoubleType(), True),

    StructField("cvv", StringType(), True),

    StructField("issue_date", DateType(), True),
    StructField("expiry_date", DateType(), True),

    StructField("status", StringType(), True),
])

# ==========================================================
# Auto Loader schema hints
# ==========================================================

CARD_SCHEMA_HINTS = """
card_id STRING,
account_id STRING,
customer_id STRING,
card_number STRING,
card_type STRING,
network STRING,
credit_limit DOUBLE,
daily_limit DOUBLE,
cvv STRING,
issue_date DATE,
expiry_date DATE,
status STRING
"""