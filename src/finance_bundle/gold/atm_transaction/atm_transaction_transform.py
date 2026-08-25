from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum,
    avg,
    min,
    max,
    when,
    lit,
    round,
    to_date,
)


# ==========================================================
# Prepare current Customer records
# ==========================================================

def prepare_customer(df: DataFrame) -> DataFrame:

    return (
        df
        .filter(col("__END_AT").isNull())
        .select(
            col("customer_id").alias("customer_id"),
            col("branch_id").alias("customer_branch_id"),
            col("city").alias("customer_city"),
            col("state").alias("customer_state"),
        )
        .dropDuplicates(["customer_id"])
    )


# ==========================================================
# Prepare current Account records
# ==========================================================

def prepare_account(df: DataFrame) -> DataFrame:

    return (
        df
        .filter(col("__END_AT").isNull())
        .select(
            col("account_id").alias("account_id"),
            col("customer_id").alias("account_customer_id"),
            col("branch_id").alias("account_branch_id"),
            col("account_type"),
        )
        .dropDuplicates(["account_id"])
    )


# ==========================================================
# Prepare current Card records
# ==========================================================

def prepare_card(df: DataFrame) -> DataFrame:

    return (
        df
        .filter(col("__END_AT").isNull())
        .select(
            col("card_id").alias("card_id"),
            col("account_id").alias("card_account_id"),
            col("customer_id").alias("card_customer_id"),
            col("card_type"),
            col("network"),
        )
        .dropDuplicates(["card_id"])
    )


# ==========================================================
# Prepare Branch records
# ==========================================================

def prepare_branch(df: DataFrame) -> DataFrame:

    return (
        df
        .select(
            col("branch_id").alias("branch_id"),
            col("branch_name"),
            col("branch_code"),
            col("city").alias("branch_city"),
            col("state").alias("branch_state"),
            col("zone"),
            col("country"),
        )
        .dropDuplicates(["branch_id"])
    )


# ==========================================================
# Build enriched ATM transaction dataset
# ==========================================================

def enrich_atm_transactions(
    atm_df: DataFrame,
    customer_df: DataFrame,
    account_df: DataFrame,
    card_df: DataFrame,
    branch_df: DataFrame,
) -> DataFrame:

    customer = prepare_customer(customer_df)
    account = prepare_account(account_df)
    card = prepare_card(card_df)
    branch = prepare_branch(branch_df)

    # ------------------------------------------------------
    # ATM → Customer
    # ------------------------------------------------------

    df = (
        atm_df.alias("atm")
        .join(
            customer.alias("cust"),
            col("atm.customer_id")
            == col("cust.customer_id"),
            "left",
        )
    )

    # ------------------------------------------------------
    # ATM → Account
    # ------------------------------------------------------

    df = (
        df
        .join(
            account.alias("acc"),
            col("atm.account_id")
            == col("acc.account_id"),
            "left",
        )
    )

    # ------------------------------------------------------
    # ATM → Card
    # ------------------------------------------------------

    df = (
        df
        .join(
            card.alias("card"),
            col("atm.card_id")
            == col("card.card_id"),
            "left",
        )
    )

    # ------------------------------------------------------
    # Determine branch
    #
    # Prefer Account branch.
    # Fall back to Customer branch.
    # ------------------------------------------------------

    df = df.withColumn(
        "resolved_branch_id",
        when(
            col("acc.account_branch_id").isNotNull(),
            col("acc.account_branch_id"),
        ).otherwise(
            col("cust.customer_branch_id")
        ),
    )

    # ------------------------------------------------------
    # ATM → Branch
    # ------------------------------------------------------

    df = (
        df
        .join(
            branch.alias("br"),
            col("resolved_branch_id")
            == col("br.branch_id"),
            "left",
        )
    )

    return df.select(
        col("atm.atm_transaction_id"),
        col("atm.atm_id"),
        col("atm.card_id"),
        col("atm.account_id"),
        col("atm.customer_id"),

        col("atm.withdrawal_amount"),
        col("atm.transaction_timestamp"),
        col("atm.status"),

        col("card.card_type"),
        col("card.network"),

        col("acc.account_type"),

        col("resolved_branch_id").alias("branch_id"),

        col("br.branch_name"),
        col("br.branch_code"),
        col("br.branch_city"),
        col("br.branch_state"),
        col("br.zone"),
        col("br.country"),
    )


# ==========================================================
# Build ATM Summary
# ==========================================================

def build_atm_summary(
    df: DataFrame,
) -> DataFrame:

    # ------------------------------------------------------
    # Transaction date
    # ------------------------------------------------------

    df = df.withColumn(
        "transaction_date",
        to_date(
            col("transaction_timestamp")
        ),
    )

    # ------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------

    summary = (
        df
        .groupBy(
            "atm_id",
            "branch_id",
            "branch_name",
            "branch_code",
            "branch_city",
            "branch_state",
            "zone",
            "country",
            "transaction_date",
        )
        .agg(

            # ----------------------------------------------
            # Transaction volume
            # ----------------------------------------------

            count("*").alias(
                "total_transactions"
            ),

            countDistinct(
                "customer_id"
            ).alias(
                "unique_customers"
            ),

            countDistinct(
                "account_id"
            ).alias(
                "unique_accounts"
            ),

            countDistinct(
                "card_id"
            ).alias(
                "unique_cards"
            ),

            # ----------------------------------------------
            # Status metrics
            # ----------------------------------------------

            sum(
                when(
                    col("status") == "SUCCESS",
                    1,
                ).otherwise(0)
            ).alias(
                "successful_transactions"
            ),

            sum(
                when(
                    col("status") == "FAILED",
                    1,
                ).otherwise(0)
            ).alias(
                "failed_transactions"
            ),

            sum(
                when(
                    col("status") == "DECLINED",
                    1,
                ).otherwise(0)
            ).alias(
                "declined_transactions"
            ),

            # ----------------------------------------------
            # Withdrawal metrics
            # ----------------------------------------------

            round(
                sum("withdrawal_amount"),
                2,
            ).alias(
                "total_withdrawal_amount"
            ),

            round(
                avg("withdrawal_amount"),
                2,
            ).alias(
                "average_withdrawal_amount"
            ),

            round(
                min("withdrawal_amount"),
                2,
            ).alias(
                "minimum_withdrawal_amount"
            ),

            round(
                max("withdrawal_amount"),
                2,
            ).alias(
                "maximum_withdrawal_amount"
            ),
        )
    )

    # ======================================================
    # KPI calculations
    # ======================================================

    summary = (
        summary

        # --------------------------------------------------
        # Success rate
        # --------------------------------------------------

        .withColumn(
            "success_rate",
            round(
                when(
                    col("total_transactions") > 0,
                    (
                        col("successful_transactions")
                        / col("total_transactions")
                    ) * 100,
                ).otherwise(0),
                2,
            ),
        )

        # --------------------------------------------------
        # Failure rate
        # --------------------------------------------------

        .withColumn(
            "failure_rate",
            round(
                when(
                    col("total_transactions") > 0,
                    (
                        col("failed_transactions")
                        / col("total_transactions")
                    ) * 100,
                ).otherwise(0),
                2,
            ),
        )

        # --------------------------------------------------
        # Decline rate
        # --------------------------------------------------

        .withColumn(
            "decline_rate",
            round(
                when(
                    col("total_transactions") > 0,
                    (
                        col("declined_transactions")
                        / col("total_transactions")
                    ) * 100,
                ).otherwise(0),
                2,
            ),
        )
    )

    # ======================================================
    # Business rule validation
    # ======================================================

    summary = (
        summary

        # Total status counts must equal total transactions
        .withColumn(
            "status_count_valid",
            (
                col("successful_transactions")
                + col("failed_transactions")
                + col("declined_transactions")
            )
            == col("total_transactions"),
        )

        # Amount must never be negative
        .withColumn(
            "amount_valid",
            col("total_withdrawal_amount") >= 0,
        )

        # Percentage validation
        .withColumn(
            "rate_valid",
            (
                (
                    col("success_rate")
                    + col("failure_rate")
                    + col("decline_rate")
                )
                <= 100.01
            ),
        )
    )

    # ======================================================
    # Final Gold validation
    # ======================================================

    return (
        summary
        .filter(
            col("status_count_valid")
            & col("amount_valid")
            & col("rate_valid")
        )
        .drop(
            "status_count_valid",
            "amount_valid",
            "rate_valid",
        )
    )