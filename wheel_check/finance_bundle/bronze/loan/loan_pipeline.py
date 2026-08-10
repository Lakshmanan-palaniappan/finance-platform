from pyspark import pipelines as dp

from finance_bundle.bronze.loan.loan_ingestion import read_loan_data

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables


@dp.table(
    name=Catalog.bronze(Tables.LOAN),
    comment="Bronze Loan Table"
)
def loan_bronze():

    return read_loan_data()