from pyspark import pipelines as dp

from bronze.loan.loan_ingestion import read_loan_data

from common.catalog import (
    CATALOG,
    BRONZE_SCHEMA
)

from common.table_names import LOAN_BRONZE_TABLE


@dp.table(
    name=f"{CATALOG}.{BRONZE_SCHEMA}.{LOAN_BRONZE_TABLE}",
    comment="Bronze Loan Table"
)
def loan_bronze():

    return read_loan_data()