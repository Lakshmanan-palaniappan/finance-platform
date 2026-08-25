from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
)


# ==========================================================
# Login Activity Source Schema
# ==========================================================

LOGIN_ACTIVITY_SCHEMA = StructType([

    StructField(
        "login_id",
        StringType(),
        False,
    ),

    StructField(
        "customer_id",
        StringType(),
        False,
    ),

    StructField(
        "login_timestamp",
        TimestampType(),
        False,
    ),

    StructField(
        "device",
        StringType(),
        True,
    ),

    StructField(
        "ip_address",
        StringType(),
        True,
    ),

    StructField(
        "latitude",
        DoubleType(),
        True,
    ),

    StructField(
        "longitude",
        DoubleType(),
        True,
    ),

    StructField(
        "city",
        StringType(),
        True,
    ),

    StructField(
        "country",
        StringType(),
        True,
    ),

    StructField(
        "login_status",
        StringType(),
        True,
    ),
])


# ==========================================================
# Auto Loader Schema Hints
# ==========================================================

LOGIN_ACTIVITY_SCHEMA_HINTS = """
login_id STRING,
customer_id STRING,
login_timestamp TIMESTAMP,
device STRING,
ip_address STRING,
latitude DOUBLE,
longitude DOUBLE,
city STRING,
country STRING,
login_status STRING
"""