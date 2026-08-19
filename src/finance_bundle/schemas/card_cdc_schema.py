from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)


CARD_CDC_SCHEMA = StructType(
    [
        StructField("entity", StringType(), True),
        StructField("operation", StringType(), True),
        StructField("card_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("old_status", StringType(), True),
        StructField("new_status", StringType(), True),
        StructField("card_type", StringType(), True),
        StructField("network", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("batch_id", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("change_timestamp", StringType(), True),
    ]
)