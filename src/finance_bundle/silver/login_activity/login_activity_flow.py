from pyspark.sql import DataFrame


# ==========================================================
# Login Activity Streaming Rules
# ==========================================================

def apply_login_activity_streaming_rules(
    df: DataFrame,
) -> DataFrame:

    # ------------------------------------------------------
    # Watermark
    #
    # Login events arriving up to 15 minutes late
    # are accepted.
    # ------------------------------------------------------

    df = df.withWatermark(
        "login_timestamp",
        "15 minutes",
    )

    # ------------------------------------------------------
    # Deduplicate by login_id
    # ------------------------------------------------------

    df = df.dropDuplicates(
        ["login_id"]
    )

    return df