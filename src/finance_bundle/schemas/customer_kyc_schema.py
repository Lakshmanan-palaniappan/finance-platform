from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
)


# ==========================================================
# Customer KYC Bronze Source Schema
# ==========================================================
#
# This schema represents the ACTUAL source CSV.
#
# Source columns:
# kyc_id
# customer_id
# document_type
# pan_number
# aadhaar_number
# verification_date
# verified_by
# status
#
# Business/derived columns such as pan_verified,
# aadhaar_verified and kyc_status are created in Silver.
# ==========================================================

CUSTOMER_KYC_SCHEMA = StructType([

    StructField(
        "kyc_id",
        StringType(),
        False,
    ),

    StructField(
        "customer_id",
        StringType(),
        False,
    ),

    StructField(
        "document_type",
        StringType(),
        True,
    ),

    StructField(
        "pan_number",
        StringType(),
        True,
    ),

    StructField(
        "aadhaar_number",
        StringType(),
        True,
    ),

    StructField(
        "verification_date",
        DateType(),
        True,
    ),

    StructField(
        "verified_by",
        StringType(),
        True,
    ),

    StructField(
        "status",
        StringType(),
        True,
    ),
])


# ==========================================================
# Auto Loader Schema Hints
# ==========================================================

CUSTOMER_KYC_SCHEMA_HINTS = """
kyc_id STRING,
customer_id STRING,
document_type STRING,
pan_number STRING,
aadhaar_number STRING,
verification_date DATE,
verified_by STRING,
status STRING
"""