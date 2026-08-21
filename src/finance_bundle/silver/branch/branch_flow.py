"""
Branch Silver Lakeflow SDP Pipeline.

Bronze Branch
      |
      v
Validation
      |
      +--------------------+
      |                    |
      v                    v
Valid Records        Quarantine
      |                    |
      v                    v
Silver Branch       Branch Quarantine
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables

from finance_bundle.silver.branch.branch_transform import (
    normalize_branch,
)


# ==========================================================
# Table Names
# ==========================================================

BRONZE_BRANCH = Catalog.bronze(
    Tables.BRANCH
)

SILVER_BRANCH = Catalog.silver(
    Tables.BRANCH
)

SILVER_BRANCH_QUARANTINE = Catalog.silver(
    "branch_quarantine"
)


# ==========================================================
# Bronze Branch -> Silver Transformation
# ==========================================================

@dp.temporary_view(
    name="branch_silver_source",
)
@dp.expect(
    "valid_branch_id",
    "branch_id IS NOT NULL",
)
@dp.expect(
    "valid_branch_code",
    "branch_code IS NOT NULL",
)
def branch_silver_source():

    df = dp.read_stream(
        BRONZE_BRANCH
    )

    # ------------------------------------------------------
    # Apply transformations
    # ------------------------------------------------------

    df = normalize_branch(df)

    # ------------------------------------------------------
    # Add audit timestamp
    # ------------------------------------------------------

    df = df.withColumn(
        "silver_processed_timestamp",
        F.current_timestamp(),
    )

    return df


# ==========================================================
# Silver Branch Table
# ==========================================================

dp.create_streaming_table(

    name=SILVER_BRANCH,

    comment=(
        "Silver Branch table containing "
        "validated, standardized and deduplicated "
        "branch records."
    ),
)


# ==========================================================
# Silver Branch Flow
# ==========================================================

@dp.append_flow(
    target=SILVER_BRANCH,
    name="branch_silver_flow",
)
def branch_silver_flow():

    return dp.read_stream(
        "branch_silver_source"
    )


# ==========================================================
# Branch Quarantine
# ==========================================================

@dp.temporary_view(
    name="branch_quarantine_source",
)
def branch_quarantine_source():

    df = dp.read_stream(
        BRONZE_BRANCH
    )

    # ------------------------------------------------------
    # Apply same transformations
    # ------------------------------------------------------

    df = normalize_branch(df)

    # ------------------------------------------------------
    # Identify invalid records
    # ------------------------------------------------------

    invalid_df = df.filter(
        F.col("branch_id").isNull()
        |
        F.col("branch_code").isNull()
    )

    # ------------------------------------------------------
    # Add quarantine metadata
    # ------------------------------------------------------

    invalid_df = (
        invalid_df

        .withColumn(
            "quarantine_reason",
            F.when(
                F.col("branch_id").isNull(),
                F.lit("Missing branch_id"),
            )
            .when(
                F.col("branch_code").isNull(),
                F.lit("Missing branch_code"),
            )
            .otherwise(
                F.lit("Validation failure")
            ),
        )

        .withColumn(
            "quarantine_timestamp",
            F.current_timestamp(),
        )
    )

    return invalid_df


# ==========================================================
# Quarantine Table
# ==========================================================

dp.create_streaming_table(

    name=SILVER_BRANCH_QUARANTINE,

    comment=(
        "Quarantine table containing "
        "Branch records that failed Silver "
        "validation rules."
    ),
)


# ==========================================================
# Quarantine Flow
# ==========================================================

@dp.append_flow(
    target=SILVER_BRANCH_QUARANTINE,
    name="branch_quarantine_flow",
)
def branch_quarantine_flow():

    return dp.read_stream(
        "branch_quarantine_source"
    )