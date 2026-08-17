from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
    DateType,
)


ACCOUNT_SCHEMA = StructType([

    StructField(
        "account_id",
        StringType(),
        False,
    ),

    StructField(
        "account_number",
        StringType(),
        False,
    ),

    StructField(
        "customer_id",
        StringType(),
        False,
    ),

    StructField(
        "branch_id",
        StringType(),
        True,
    ),

    StructField(
        "account_type",
        StringType(),
        True,
    ),

    StructField(
        "balance",
        DecimalType(18, 2),
        True,
    ),

    StructField(
        "minimum_balance",
        DecimalType(18, 2),
        True,
    ),

    StructField(
        "interest_rate",
        DecimalType(5, 2),
        True,
    ),

    StructField(
        "opened_date",
        DateType(),
        True,
    ),

    StructField(
        "account_status",
        StringType(),
        True,
    ),
])


ACCOUNT_SCHEMA_HINTS = """
account_id STRING,
account_number STRING,
customer_id STRING,
branch_id STRING,
account_type STRING,
balance DECIMAL(18,2),
minimum_balance DECIMAL(18,2),
interest_rate DECIMAL(5,2),
opened_date DATE,
account_status STRING
"""