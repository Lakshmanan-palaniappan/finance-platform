from pyspark import pipelines as dp
from pyspark.sql import functions as F

from finance_bundle.common.catalog import Catalog
from finance_bundle.common.table_names import Tables


# ==========================================================
# Read Silver Branch
# ==========================================================

def read_silver_branch():

    return dp.read(
        Catalog.silver(Tables.BRANCH)
    )


# ==========================================================
# Get Current Branch Records
# ==========================================================

def get_current_branches(df):

    if "_is_current" in df.columns:

        return df.filter(
            F.col("_is_current") == F.lit(True)
        )

    if "is_current" in df.columns:

        return df.filter(
            F.col("is_current") == F.lit(True)
        )

    return df


# ==========================================================
# Apply Business Rules
# ==========================================================

def apply_business_rules(df):

    return (
        df

        # --------------------------------------------------
        # Standardize branch status
        # --------------------------------------------------

        .withColumn(
            "branch_status",
            F.upper(
                F.trim(
                    F.col("status")
                )
            ),
        )

        # --------------------------------------------------
        # Standardize zone
        # --------------------------------------------------

        .withColumn(
            "branch_zone",
            F.upper(
                F.trim(
                    F.col("zone")
                )
            ),
        )

        # --------------------------------------------------
        # Standardize state
        # --------------------------------------------------

        .withColumn(
            "branch_state",
            F.initcap(
                F.trim(
                    F.col("state")
                )
            ),
        )

        # --------------------------------------------------
        # Standardize city
        # --------------------------------------------------

        .withColumn(
            "branch_city",
            F.initcap(
                F.trim(
                    F.col("city")
                )
            ),
        )

        # --------------------------------------------------
        # Active branch flag
        # --------------------------------------------------

        .withColumn(
            "is_active",
            F.when(
                F.col("branch_status") == "ACTIVE",
                1,
            ).otherwise(0),
        )

        # --------------------------------------------------
        # Inactive branch flag
        # --------------------------------------------------

        .withColumn(
            "is_inactive",
            F.when(
                F.col("branch_status") == "INACTIVE",
                1,
            ).otherwise(0),
        )

        # --------------------------------------------------
        # Operational branch flag
        # --------------------------------------------------

        .withColumn(
            "is_operational",
            F.when(
                F.col("branch_status") == "ACTIVE",
                1,
            ).otherwise(0),
        )
    )


# ==========================================================
# Create Branch Gold Metrics
# ==========================================================

def create_branch_metrics(df):

    return (
        df

        .groupBy(
            "branch_state",
            "branch_zone",
            "branch_status",
        )

        .agg(

            # ------------------------------------------------
            # Total branches
            # ------------------------------------------------

            F.countDistinct(
                "branch_id"
            ).alias(
                "total_branches"
            ),

            # ------------------------------------------------
            # Active branches
            # ------------------------------------------------

            F.sum(
                "is_active"
            ).alias(
                "active_branches"
            ),

            # ------------------------------------------------
            # Inactive branches
            # ------------------------------------------------

            F.sum(
                "is_inactive"
            ).alias(
                "inactive_branches"
            ),

            # ------------------------------------------------
            # Operational branches
            # ------------------------------------------------

            F.sum(
                "is_operational"
            ).alias(
                "operational_branches"
            ),

            # ------------------------------------------------
            # Number of cities
            # ------------------------------------------------

            F.countDistinct(
                "branch_city"
            ).alias(
                "total_cities"
            ),

            # ------------------------------------------------
            # Number of branch codes
            # ------------------------------------------------

            F.countDistinct(
                "branch_code"
            ).alias(
                "unique_branch_codes"
            ),

            # ------------------------------------------------
            # Number of IFSC codes
            # ------------------------------------------------

            F.countDistinct(
                "ifsc_code"
            ).alias(
                "unique_ifsc_codes"
            ),
        )
    )


# ==========================================================
# Final Gold Transformation
# ==========================================================

def transform_branch_gold():

    # ------------------------------------------------------
    # Read Silver
    # ------------------------------------------------------

    silver_df = read_silver_branch()

    # ------------------------------------------------------
    # Get current records
    # ------------------------------------------------------

    current_df = get_current_branches(
        silver_df
    )

    # ------------------------------------------------------
    # Apply business rules
    # ------------------------------------------------------

    business_df = apply_business_rules(
        current_df
    )

    # ------------------------------------------------------
    # Create Gold metrics
    # ------------------------------------------------------

    gold_df = create_branch_metrics(
        business_df
    )

    # ======================================================
    # KPI calculations
    # ======================================================

    gold_df = (
        gold_df

        # --------------------------------------------------
        # Active branch percentage
        # --------------------------------------------------

        .withColumn(
            "active_branch_percentage",
            F.when(
                F.col("total_branches") > 0,

                (
                    F.col("active_branches")
                    /
                    F.col("total_branches")
                ) * 100,

            ).otherwise(0),
        )

        # --------------------------------------------------
        # Inactive branch percentage
        # --------------------------------------------------

        .withColumn(
            "inactive_branch_percentage",
            F.when(
                F.col("total_branches") > 0,

                (
                    F.col("inactive_branches")
                    /
                    F.col("total_branches")
                ) * 100,

            ).otherwise(0),
        )

        # --------------------------------------------------
        # Operational branch percentage
        # --------------------------------------------------

        .withColumn(
            "operational_branch_percentage",
            F.when(
                F.col("total_branches") > 0,

                (
                    F.col("operational_branches")
                    /
                    F.col("total_branches")
                ) * 100,

            ).otherwise(0),
        )

        # --------------------------------------------------
        # Branch performance KPI
        # --------------------------------------------------

        .withColumn(
            "branch_performance_percentage",
            F.when(
                F.col("total_branches") > 0,

                (
                    F.col("operational_branches")
                    /
                    F.col("total_branches")
                ) * 100,

            ).otherwise(0),
        )
    )

    return gold_df