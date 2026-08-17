from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
)


ACCOUNT_CDC_SCHEMA = StructType([

    StructField(
        "event_id",
        StringType(),
        False,
    ),

    StructField(
        "entity",
        StringType(),
        False,
    ),

    StructField(
        "operation",
        StringType(),
        False,
    ),

    StructField(
        "account_id",
        StringType(),
        False,
    ),

    StructField(
        "attribute",
        StringType(),
        True,
    ),

    StructField(
        "old_value",
        StringType(),
        True,
    ),

    StructField(
        "new_value",
        StringType(),
        True,
    ),

    StructField(
        "batch_id",
        StringType(),
        True,
    ),

    StructField(
        "source_system",
        StringType(),
        True,
    ),

    StructField(
        "event_timestamp",
        TimestampType(),
        True,
    ),

    StructField(
        "change_timestamp",
        TimestampType(),
        True,
    ),
])