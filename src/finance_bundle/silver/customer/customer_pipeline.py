"""
Customer Silver Lakeflow SDP Pipeline.

Bronze Customer
       +
Bronze Customer CDC
       |
       v
customer_cdc_source
       |
       v
Silver Customer SCD2
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables


# ==========================================================
# Table Names
# ==========================================================

BRONZE_CUSTOMER = Catalog.bronze(
    Tables.CUSTOMER
)

BRONZE_CUSTOMER_CDC = Catalog.bronze(
    Tables.CUSTOMER_CDC
)

SILVER_CUSTOMER = Catalog.silver(
    Tables.CUSTOMER
)


# ==========================================================
# Bronze Customer CDC Source
# ==========================================================

@dp.temporary_view(
    name="customer_cdc_source",
)
def customer_cdc_source():

    customer_df = dp.read(
        BRONZE_CUSTOMER
    )

    cdc_df = dp.read_stream(
        BRONZE_CUSTOMER_CDC
    )

    # ------------------------------------------------------
    # Normalize customer
    # ------------------------------------------------------

    for column_name in customer_df.columns:

        normalized = (
            column_name
            .strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
        )

        if normalized != column_name:

            customer_df = customer_df.withColumnRenamed(
                column_name,
                normalized,
            )

    # ------------------------------------------------------
    # Normalize CDC
    # ------------------------------------------------------

    for column_name in cdc_df.columns:

        normalized = (
            column_name
            .strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
        )

        if normalized != column_name:

            cdc_df = cdc_df.withColumnRenamed(
                column_name,
                normalized,
            )

    # ------------------------------------------------------
    # Normalize CDC values
    # ------------------------------------------------------

    cdc_df = (
        cdc_df
        .withColumn(
            "entity",
            F.upper(
                F.trim(
                    F.col("entity")
                )
            ),
        )
        .withColumn(
            "operation",
            F.lower(
                F.trim(
                    F.col("operation")
                )
            ),
        )
        .withColumn(
            "customer_id",
            F.trim(
                F.col("customer_id")
            ),
        )
    )

    # ------------------------------------------------------
    # Normalize customer values
    # ------------------------------------------------------

    string_columns = [
        "customer_id",
        "branch_id",
        "first_name",
        "last_name",
        "gender",
        "mobile_number",
        "email",
        "pan_number",
        "aadhaar_number",
        "occupation",
        "city",
        "state",
        "customer_status",
    ]

    for column_name in string_columns:

        if column_name in customer_df.columns:

            customer_df = customer_df.withColumn(
                column_name,
                F.trim(
                    F.col(column_name).cast("string")
                ),
            )

    if "email" in customer_df.columns:

        customer_df = customer_df.withColumn(
            "email",
            F.lower(
                F.col("email")
            ),
        )

    if "customer_status" in customer_df.columns:

        customer_df = customer_df.withColumn(
            "customer_status",
            F.upper(
                F.col("customer_status")
            ),
        )

    if "dob" in customer_df.columns:

        customer_df = customer_df.withColumn(
            "dob",
            F.to_date(
                F.col("dob")
            ),
        )

    if "annual_income" in customer_df.columns:

        customer_df = customer_df.withColumn(
            "annual_income",
            F.col("annual_income").cast("double"),
        )

    # ------------------------------------------------------
    # Remove duplicate customer state
    # ------------------------------------------------------

    customer_df = customer_df.dropDuplicates(
        ["customer_id"]
    )

    # ------------------------------------------------------
    # Join CDC events to current customer state
    # ------------------------------------------------------

    joined_df = (
        cdc_df.alias("cdc")
        .join(
            customer_df.alias("customer"),

            F.col("cdc.customer_id")
            ==
            F.col("customer.customer_id"),

            "left",
        )
    )

    # ------------------------------------------------------
    # Full customer CDC record
    # ------------------------------------------------------

    return joined_df.select(

        F.col(
            "customer.customer_id"
        ).alias("customer_id"),

        F.col(
            "customer.branch_id"
        ).alias("branch_id"),

        F.col(
            "customer.first_name"
        ).alias("first_name"),

        F.col(
            "customer.last_name"
        ).alias("last_name"),

        F.col(
            "customer.gender"
        ).alias("gender"),

        F.col(
            "customer.dob"
        ).alias("dob"),

        F.col(
            "customer.mobile_number"
        ).alias("mobile_number"),

        F.col(
            "customer.email"
        ).alias("email"),

        F.col(
            "customer.pan_number"
        ).alias("pan_number"),

        F.col(
            "customer.aadhaar_number"
        ).alias("aadhaar_number"),

        F.col(
            "customer.occupation"
        ).alias("occupation"),

        F.col(
            "customer.annual_income"
        ).alias("annual_income"),

        F.col(
            "customer.city"
        ).alias("city"),

        F.col(
            "customer.state"
        ).alias("state"),

        F.col(
            "customer.customer_status"
        ).alias("customer_status"),

        # --------------------------------------------------
        # CDC metadata
        # --------------------------------------------------

        F.col(
            "cdc.operation"
        ).alias("_operation"),

        F.col(
            "cdc.change_timestamp"
        ).alias("_sequence_timestamp"),

        F.col(
            "cdc.event_timestamp"
        ).alias("_event_timestamp"),

        F.col(
            "cdc.event_id"
        ).alias("_event_id"),
    )


# ==========================================================
# Silver Customer
# ==========================================================

dp.create_streaming_table(
    name=SILVER_CUSTOMER,

    comment=(
        "Silver Customer SCD Type 2 table"
    ),
)


# ==========================================================
# AUTO CDC
# ==========================================================

dp.create_auto_cdc_flow(

    target=SILVER_CUSTOMER,

    source="customer_cdc_source",

    keys=[
        "customer_id",
    ],

    sequence_by=F.col(
        "_sequence_timestamp"
    ),

    apply_as_deletes=(
        F.col("_operation")
        ==
        F.lit("delete")
    ),

    except_column_list=[
        "_operation",
        "_sequence_timestamp",
        "_event_timestamp",
        "_event_id",
    ],

    stored_as_scd_type=2,
)