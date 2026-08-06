from pyspark import pipelines as dp

from silver.loan.loan_transform import transform_loan


@dp.table(
    name="banking_catalog.silver.loan_silver",
    comment="Silver Loan Table"
)
def loan_silver():

    bronze_df = spark.read.table(
        "banking_catalog.bronze.loan_bronze"
    )

    silver_df = transform_loan(bronze_df)

    return silver_df