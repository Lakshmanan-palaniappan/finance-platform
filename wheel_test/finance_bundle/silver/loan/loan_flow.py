from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    when,
    current_date,
    current_timestamp,
    expr
)

from finance_bundle.common.catalog import CATALOG, SILVER_SCHEMA
from finance_bundle.common.table_names import LOAN_SILVER_TABLE


BUSINESS_KEY = "loan_id"

TRACKED_COLUMNS = [
    "loan_amount",
    "interest_rate",
    "monthly_emi",
    "paid_emi",
    "remaining_emi",
    "outstanding_balance",
    "loan_to_income_ratio",
    "status"
]


def identify_cdc(incoming_df: DataFrame, existing_df: DataFrame) -> DataFrame:

    existing = existing_df.alias("e")

    incoming = incoming_df.alias("i")

    joined = incoming.join(
        existing,
        col("i.loan_id") == col("e.loan_id"),
        "left"
    )

    update_condition = (

        (col("i.loan_amount") != col("e.loan_amount")) |

        (col("i.interest_rate") != col("e.interest_rate")) |

        (col("i.monthly_emi") != col("e.monthly_emi")) |

        (col("i.paid_emi") != col("e.paid_emi")) |

        (col("i.remaining_emi") != col("e.remaining_emi")) |

        (col("i.outstanding_balance") != col("e.outstanding_balance")) |

        (col("i.loan_to_income_ratio") != col("e.loan_to_income_ratio")) |

        (col("i.status") != col("e.status"))

    )

    return (

        joined

        .withColumn(

            "operation",

            when(
                col("e.loan_id").isNull(),
                lit("INSERT")
            )

            .when(
                update_condition,
                lit("UPDATE")
            )

            .otherwise(
                lit("NO_CHANGE")
            )

        )

    )
def prepare_scd2(df: DataFrame) -> DataFrame:

    return (

        df

        .filter(
            col("operation") != "NO_CHANGE"
        )

        .withColumn(
            "effective_start_date",
            current_date()
        )

        .withColumn(
            "effective_end_date",
            lit("9999-12-31").cast("date")
        )

        .withColumn(
            "is_current",
            lit(True)
        )

        .withColumn(
            "version",
            lit(1)
        )

        .withColumn(
            "created_timestamp",
            current_timestamp()
        )

    )
def expire_existing_records(delta_table):

    (

        delta_table.alias("target")

        .update(

            condition="""

            target.loan_id IN (

                SELECT loan_id

                FROM source_updates

            )

            AND target.is_current = true

            """,

            set={

                "effective_end_date": "current_date()",

                "is_current": "false"

            }

        )

    )
