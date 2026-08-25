from pyspark import pipelines as dp

from finance_bundle.bronze.login_activity.login_activity_ingestion import (
    read_login_activity_data,
)

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables


# ==========================================================
# Bronze Login Activity
# ==========================================================

BRONZE_LOGIN_ACTIVITY = Catalog.bronze(
    Tables.LOGIN_ACTIVITY
)


@dp.table(
    name=BRONZE_LOGIN_ACTIVITY,
    comment=(
        "Bronze Login Activity streaming table "
        "ingested from ADLS using Auto Loader"
    ),
)
def login_activity_bronze():

    return read_login_activity_data()