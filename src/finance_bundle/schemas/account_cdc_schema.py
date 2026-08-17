from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
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
        "batch_id",
        StringType(),
        True,
    ),
])