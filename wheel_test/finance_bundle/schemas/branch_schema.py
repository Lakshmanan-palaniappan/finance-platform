from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

# ==========================================================
# Explicit Spark schema
# ==========================================================

branch_schema = StructType([
    StructField("branch_id", StringType(), False),
    StructField("branch_name", StringType(), True),
    StructField("branch_code", StringType(), True),
    StructField("ifsc_code", StringType(), True),
    StructField("bank_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("country", StringType(), True),
    StructField("status", StringType(), True),
])

# ==========================================================
# Auto Loader schema hints
# ==========================================================

BRANCH_SCHEMA_HINTS = """
branch_id STRING,
branch_name STRING,
branch_code STRING,
ifsc_code STRING,
bank_name STRING,
city STRING,
state STRING,
zone STRING,
country STRING,
status STRING
"""