from pyspark import pipelines as dp
from pyspark.sql import functions as F

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables

from finance_bundle.silver.login_activity.login_activity_flow import (
    apply_login_activity_streaming_rules,
)

from finance_bundle.silver.login_activity.login_activity_transform import (
    normalize_login_activity,
)


# ==========================================================
# Tables
# ==========================================================

BRONZE_LOGIN_ACTIVITY = Catalog.bronze(
    Tables.LOGIN_ACTIVITY
)

SILVER_LOGIN_ACTIVITY = Catalog.silver(
    Tables.LOGIN_ACTIVITY
)

QUARANTINE_LOGIN_ACTIVITY = Catalog.quarantine(
    Tables.LOGIN_ACTIVITY_QUARANTINE
)


# ==========================================================
# Silver Login Activity
# ==========================================================

@dp.table(
    name=SILVER_LOGIN_ACTIVITY,
    comment=(
        "Silver Login Activity streaming table "
        "with cleansing, validation, "
        "watermarking and deduplication"
    ),
)
@dp.expect(
    "login_id_not_null",
    "login_id IS NOT NULL",
)
@dp.expect(
    "customer_id_not_null",
    "customer_id IS NOT NULL",
)
@dp.expect(
    "login_timestamp_not_null",
    "login_timestamp IS NOT NULL",
)
@dp.expect(
    "login_status_valid",
    """
    login_status IS NULL
    OR login_status IN ('SUCCESS', 'FAILED')
    """,
)
@dp.expect(
    "latitude_valid",
    """
    latitude IS NULL
    OR latitude BETWEEN -90 AND 90
    """,
)
@dp.expect(
    "longitude_valid",
    """
    longitude IS NULL
    OR longitude BETWEEN -180 AND 180
    """,
)
def login_activity_silver():

    df = dp.read_stream(
        BRONZE_LOGIN_ACTIVITY
    )

    # ------------------------------------------------------
    # Normalize
    # ------------------------------------------------------

    df = normalize_login_activity(
        df
    )

    # ------------------------------------------------------
    # Watermark + deduplication
    # ------------------------------------------------------

    df = apply_login_activity_streaming_rules(
        df
    )

    # ------------------------------------------------------
    # Silver filtering
    #
    # Only enforce mandatory business keys/timestamp.
    # Optional location fields are allowed to be NULL.
    # ------------------------------------------------------

    return df.filter(
        F.col(
            "login_id"
        ).isNotNull()
        &
        F.col(
            "customer_id"
        ).isNotNull()
        &
        F.col(
            "login_timestamp"
        ).isNotNull()
    )


# ==========================================================
# Login Activity Quarantine
# ==========================================================

@dp.table(
    name=QUARANTINE_LOGIN_ACTIVITY,
    comment=(
        "Login Activity records that fail "
        "Silver validation"
    ),
)
def login_activity_quarantine():

    df = dp.read_stream(
        BRONZE_LOGIN_ACTIVITY
    )

    # ------------------------------------------------------
    # Normalize
    # ------------------------------------------------------

    df = normalize_login_activity(
        df
    )

    # ------------------------------------------------------
    # Quarantine records that violate mandatory
    # structural requirements.
    # ------------------------------------------------------

    return df.filter(
        F.col(
            "login_id"
        ).isNull()
        |
        F.col(
            "customer_id"
        ).isNull()
        |
        F.col(
            "login_timestamp"
        ).isNull()
    )