from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    DateType,
)

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("branch_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("mobile_number", StringType(), True),
    StructField("email", StringType(), True),
    StructField("pan_number", StringType(), True),
    StructField("aadhaar_number", StringType(), True),
    StructField("occupation", StringType(), True),
    StructField("annual_income", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("customer_status", StringType(), True),
])

CUSTOMER_SCHEMA_HINTS = """
customer_id STRING,
branch_id STRING,
first_name STRING,
last_name STRING,
gender STRING,
dob DATE,
mobile_number STRING,
email STRING,
pan_number STRING,
aadhaar_number STRING,
occupation STRING,
annual_income DOUBLE,
city STRING,
state STRING,
customer_status STRING
"""