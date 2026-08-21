from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ==========================================================
# Normalize Login Activity
# ==========================================================

def normalize_login_activity(
    df: DataFrame,
) -> DataFrame:

    return (
        df

        # --------------------------------------------------
        # Login ID
        # --------------------------------------------------

        .withColumn(
            "login_id",
            F.trim(
                F.col("login_id")
            ),
        )

        # --------------------------------------------------
        # Customer ID
        # --------------------------------------------------

        .withColumn(
            "customer_id",
            F.trim(
                F.col("customer_id")
            ),
        )

        # --------------------------------------------------
        # Login timestamp
        # --------------------------------------------------

        .withColumn(
            "login_timestamp",
            F.to_timestamp(
                F.col("login_timestamp")
            ),
        )

        # --------------------------------------------------
        # Device
        # --------------------------------------------------

        .withColumn(
            "device",
            F.upper(
                F.trim(
                    F.col("device")
                )
            ),
        )

        # --------------------------------------------------
        # IP address
        # --------------------------------------------------

        .withColumn(
            "ip_address",
            F.trim(
                F.col("ip_address")
            ),
        )

        # --------------------------------------------------
        # Latitude
        # --------------------------------------------------

        .withColumn(
            "latitude",
            F.col("latitude").cast(
                "double"
            ),
        )

        # --------------------------------------------------
        # Longitude
        # --------------------------------------------------

        .withColumn(
            "longitude",
            F.col("longitude").cast(
                "double"
            ),
        )

        # --------------------------------------------------
        # City
        # --------------------------------------------------

        .withColumn(
            "city",
            F.upper(
                F.trim(
                    F.col("city")
                )
            ),
        )

        # --------------------------------------------------
        # Country
        # --------------------------------------------------

        .withColumn(
            "country",
            F.upper(
                F.trim(
                    F.col("country")
                )
            ),
        )

        # --------------------------------------------------
        # Login Status
        # --------------------------------------------------

        .withColumn(
            "login_status",
            F.upper(
                F.trim(
                    F.col("login_status")
                )
            ),
        )
    )