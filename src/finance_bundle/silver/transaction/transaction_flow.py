from pyspark.sql import DataFrame


# ==========================================================
# Transaction Streaming Rules
# ==========================================================

def apply_transaction_streaming_rules(
    df: DataFrame,
) -> DataFrame:

    # ======================================================
    # Watermark
    #
    # Allow transactions arriving up to 10 minutes late.
    # ======================================================

    df = df.withWatermark(
        "transaction_timestamp",
        "10 minutes",
    )

    # ======================================================
    # Deduplication
    #
    # transaction_id is the transaction business key.
    #
    # Watermark allows Spark to clean old state.
    # ======================================================

    df = df.dropDuplicates(
        ["transaction_id"]
    )

    return df