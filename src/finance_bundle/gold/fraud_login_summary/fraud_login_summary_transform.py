from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F


# ==========================================================
# Business Rules
# ==========================================================

# Implementation assumption:
# Speeds above 900 km/h between two successful/failed
# login locations are considered impossible travel.
IMPOSSIBLE_TRAVEL_SPEED_KMH = 900.0


# ==========================================================
# Login Anomaly Details
# ==========================================================

def build_login_anomaly_details(
    login_df: DataFrame,
) -> DataFrame:

    # ------------------------------------------------------
    # Previous login for each customer
    # ------------------------------------------------------

    customer_window = (
        Window
        .partitionBy(
            "customer_id"
        )
        .orderBy(
            "login_timestamp"
        )
    )

    df = (
        login_df

        .withColumn(
            "previous_login_timestamp",
            F.lag(
                "login_timestamp"
            ).over(
                customer_window
            ),
        )

        .withColumn(
            "previous_device",
            F.lag(
                "device"
            ).over(
                customer_window
            ),
        )

        .withColumn(
            "previous_city",
            F.lag(
                "city"
            ).over(
                customer_window
            ),
        )

        .withColumn(
            "previous_latitude",
            F.lag(
                "latitude"
            ).over(
                customer_window
            ),
        )

        .withColumn(
            "previous_longitude",
            F.lag(
                "longitude"
            ).over(
                customer_window
            ),
        )
    )

    # ------------------------------------------------------
    # Failed Login
    # ------------------------------------------------------

    df = df.withColumn(
        "failed_login_flag",
        F.when(
            F.col("login_status") == "FAILED",
            1,
        ).otherwise(0),
    )

    # ------------------------------------------------------
    # Device Change
    # ------------------------------------------------------

    df = df.withColumn(
        "device_change_flag",
        F.when(
            F.col(
                "previous_device"
            ).isNotNull()
            &
            F.col("device").isNotNull()
            &
            (
                F.col("device")
                != F.col("previous_device")
            ),
            1,
        ).otherwise(0),
    )

    # ------------------------------------------------------
    # Multiple-City Login
    #
    # A customer logging into different cities
    # across consecutive login events is flagged.
    # ------------------------------------------------------

    df = df.withColumn(
        "multiple_city_flag",
        F.when(
            F.col(
                "previous_city"
            ).isNotNull()
            &
            F.col("city").isNotNull()
            &
            (
                F.col("city")
                != F.col("previous_city")
            ),
            1,
        ).otherwise(0),
    )

    # ------------------------------------------------------
    # Time Difference
    # ------------------------------------------------------

    df = df.withColumn(
        "elapsed_hours",
        (
            F.col("login_timestamp").cast("long")
            -
            F.col(
                "previous_login_timestamp"
            ).cast("long")
        )
        / 3600.0,
    )

    # ------------------------------------------------------
    # Haversine Distance
    #
    # Calculate distance between current and previous
    # login locations.
    # ------------------------------------------------------

    latitude_1 = F.radians(
        F.col("previous_latitude")
    )

    latitude_2 = F.radians(
        F.col("latitude")
    )

    longitude_1 = F.radians(
        F.col("previous_longitude")
    )

    longitude_2 = F.radians(
        F.col("longitude")
    )

    delta_latitude = (
        latitude_2
        -
        latitude_1
    )

    delta_longitude = (
        longitude_2
        -
        longitude_1
    )

    haversine_a = (
        F.pow(
            F.sin(
                delta_latitude / 2
            ),
            2,
        )
        +
        (
            F.cos(latitude_1)
            *
            F.cos(latitude_2)
            *
            F.pow(
                F.sin(
                    delta_longitude / 2
                ),
                2,
            )
        )
    )

    distance_km = (
        6371.0
        *
        2
        *
        F.asin(
            F.sqrt(
                F.least(
                    haversine_a,
                    F.lit(1.0),
                )
            )
        )
    )

    df = df.withColumn(
        "distance_from_previous_km",
        F.when(
            F.col(
                "previous_latitude"
            ).isNotNull()
            &
            F.col(
                "previous_longitude"
            ).isNotNull()
            &
            F.col("latitude").isNotNull()
            &
            F.col("longitude").isNotNull(),
            distance_km,
        ),
    )

    # ------------------------------------------------------
    # Travel Speed
    # ------------------------------------------------------

    df = df.withColumn(
        "travel_speed_kmh",
        F.when(
            (
                F.col(
                    "elapsed_hours"
                ) > 0
            )
            &
            F.col(
                "distance_from_previous_km"
            ).isNotNull(),
            F.col(
                "distance_from_previous_km"
            )
            /
            F.col(
                "elapsed_hours"
            ),
        ),
    )

    # ------------------------------------------------------
    # Impossible Travel
    # ------------------------------------------------------

    df = df.withColumn(
        "impossible_travel_flag",
        F.when(
            (
                F.col(
                    "travel_speed_kmh"
                )
                >
                IMPOSSIBLE_TRAVEL_SPEED_KMH
            ),
            1,
        ).otherwise(0),
    )

    return df


# ==========================================================
# Fraud Login Summary
# ==========================================================

def build_fraud_login_summary(
    login_df: DataFrame,
) -> DataFrame:

    df = build_login_anomaly_details(
        login_df
    )

    return df.agg(

        # --------------------------------------------------
        # Total login events
        # --------------------------------------------------

        F.count(
            "*"
        ).alias(
            "total_logins"
        ),

        # --------------------------------------------------
        # Failed login events
        # --------------------------------------------------

        F.sum(
            "failed_login_flag"
        ).alias(
            "failed_logins"
        ),

        # --------------------------------------------------
        # Device changes
        # --------------------------------------------------

        F.sum(
            "device_change_flag"
        ).alias(
            "device_changes"
        ),

        # --------------------------------------------------
        # Multiple-city logins
        # --------------------------------------------------

        F.sum(
            "multiple_city_flag"
        ).alias(
            "multiple_city_logins"
        ),

        # --------------------------------------------------
        # Impossible travel events
        # --------------------------------------------------

        F.sum(
            "impossible_travel_flag"
        ).alias(
            "impossible_travel_events"
        ),

        # --------------------------------------------------
        # Customers with failed logins
        # --------------------------------------------------

        F.countDistinct(
            F.when(
                F.col(
                    "failed_login_flag"
                ) == 1,
                F.col(
                    "customer_id"
                ),
            )
        ).alias(
            "customers_with_failed_logins"
        ),

        # --------------------------------------------------
        # Customers with device changes
        # --------------------------------------------------

        F.countDistinct(
            F.when(
                F.col(
                    "device_change_flag"
                ) == 1,
                F.col(
                    "customer_id"
                ),
            )
        ).alias(
            "customers_with_device_changes"
        ),

        # --------------------------------------------------
        # Customers with multiple cities
        # --------------------------------------------------

        F.countDistinct(
            F.when(
                F.col(
                    "multiple_city_flag"
                ) == 1,
                F.col(
                    "customer_id"
                ),
            )
        ).alias(
            "customers_with_multiple_city_logins"
        ),

        # --------------------------------------------------
        # Customers with impossible travel
        # --------------------------------------------------

        F.countDistinct(
            F.when(
                F.col(
                    "impossible_travel_flag"
                ) == 1,
                F.col(
                    "customer_id"
                ),
            )
        ).alias(
            "customers_with_impossible_travel"
        ),
    )