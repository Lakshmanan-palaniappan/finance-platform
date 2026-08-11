from pyspark import pipelines as dp

from finance_bundle.bronze.branch.branch_ingestion import (
    read_branch_data,
)

from finance_bundle.common.catalog import (
    Catalog,
)

from finance_bundle.common.table_names import (
    Tables,
)


@dp.table(
    name=Catalog.bronze(Tables.BRANCH),
    comment="Bronze Branch Table",
)
def branch_bronze():

    return read_branch_data()