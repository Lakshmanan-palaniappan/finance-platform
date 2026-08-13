from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    DateType,
)


EXCHANGE_RATE_SCHEMA = StructType([
    StructField("base_currency", StringType(), False),
    StructField("target_currency", StringType(), False),
    StructField("exchange_rate", DoubleType(), True),
    StructField("effective_date", DateType(), True),
])


# Auto Loader schema hints
EXCHANGE_RATE_SCHEMA_HINTS = """
base_currency STRING,
target_currency STRING,
exchange_rate DOUBLE,
effective_date DATE
"""
