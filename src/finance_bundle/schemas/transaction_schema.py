from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
    TimestampType,
)


# ==========================================================
# Transaction Source Schema
# ==========================================================

TRANSACTION_SCHEMA = StructType([

    StructField(
        "transaction_id",
        StringType(),
        False,
    ),

    StructField(
        "account_id",
        StringType(),
        False,
    ),

    StructField(
        "transaction_timestamp",
        TimestampType(),
        False,
    ),

    StructField(
        "transaction_type",
        StringType(),
        True,
    ),

    StructField(
        "amount",
        DecimalType(18, 2),
        True,
    ),

    StructField(
        "currency",
        StringType(),
        True,
    ),

    StructField(
        "merchant_id",
        StringType(),
        True,
    ),

    StructField(
        "channel",
        StringType(),
        True,
    ),

    StructField(
        "branch_id",
        StringType(),
        True,
    ),

    StructField(
        "transaction_status",
        StringType(),
        True,
    ),
])


# ==========================================================
# Auto Loader Schema Hints
# ==========================================================

TRANSACTION_SCHEMA_HINTS = """
transaction_id STRING,
account_id STRING,
transaction_timestamp TIMESTAMP,
transaction_type STRING,
amount DECIMAL(18,2),
currency STRING,
merchant_id STRING,
channel STRING,
branch_id STRING,
transaction_status STRING
"""