from pyspark import pipelines as dp

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables

from finance_bundle.gold.fraud_login_summary.fraud_login_summary_transform import (
    build_fraud_login_summary,
)


# ==========================================================
# Source
# ==========================================================

SILVER_LOGIN_ACTIVITY = Catalog.silver(
    Tables.LOGIN_ACTIVITY
)


# ==========================================================
# Target
# ==========================================================

GOLD_FRAUD_LOGIN_SUMMARY = Catalog.gold(
    Tables.FRAUD_LOGIN_SUMMARY
)


# ==========================================================
# Fraud Login Summary
# ==========================================================

@dp.materialized_view(
    name=GOLD_FRAUD_LOGIN_SUMMARY,
    comment=(
        "Fraud login summary containing "
        "failed logins, device changes, "
        "multiple-city logins and "
        "impossible travel events"
    ),
)
def fraud_login_summary():

    df = dp.read(
        SILVER_LOGIN_ACTIVITY
    )

    return build_fraud_login_summary(
        df
    )