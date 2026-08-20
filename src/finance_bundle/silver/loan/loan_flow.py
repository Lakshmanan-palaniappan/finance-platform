from pyspark import pipelines as dp

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables


# ==========================================================
# BUSINESS KEY
# ==========================================================

BUSINESS_KEY = "loan_id"


# ==========================================================
# CREATE SCD TYPE 2 TARGET
# ==========================================================

dp.create_streaming_table(
    name=Catalog.silver(Tables.LOAN),
    comment="Silver Loan table maintained using SCD Type 2"
)


# ==========================================================
# AUTO CDC / SCD TYPE 2
# ==========================================================

dp.create_auto_cdc_flow(
    target=Catalog.silver(Tables.LOAN),

    source="loan_cdc_prepared",

    keys=[
        BUSINESS_KEY
    ],

    sequence_by="change_timestamp",

    apply_as_deletes=(
        "operation = 'delete'"
    ),

    except_column_list=[
        "operation",
        "event_id",
        "batch_id",
        "source_system",
        "event_timestamp",
        "change_timestamp",
    ],

    stored_as_scd_type=2,
)