"""
Global project constants.
Do not put environment-specific values here.
"""

# -------------------------------------------------------------------
# Metadata Columns
# -------------------------------------------------------------------

INGESTION_TIMESTAMP = "ingestion_timestamp"
SOURCE_FILE = "source_file"
LOAD_DATE = "load_date"
PIPELINE_RUN_ID = "pipeline_run_id"
BATCH_ID = "batch_id"
SOURCE_SYSTEM = "source_system"

# -------------------------------------------------------------------
# Delta Metadata
# -------------------------------------------------------------------

IS_CURRENT = "is_current"
EFFECTIVE_FROM = "effective_from"
EFFECTIVE_TO = "effective_to"

# -------------------------------------------------------------------
# File Formats
# -------------------------------------------------------------------

CSV = "csv"
DELTA = "delta"
PARQUET = "parquet"

# -------------------------------------------------------------------
# Layer Names
# -------------------------------------------------------------------

BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"

# -------------------------------------------------------------------
# Load Types
# -------------------------------------------------------------------

FULL = "FULL"
INCREMENTAL = "INCREMENTAL"

# -------------------------------------------------------------------
# Customer Status
# -------------------------------------------------------------------

ACTIVE = "ACTIVE"
INACTIVE = "INACTIVE"
BLOCKED = "BLOCKED"
CLOSED = "CLOSED"

# -------------------------------------------------------------------
# Account Status
# -------------------------------------------------------------------

OPEN = "OPEN"
FROZEN = "FROZEN"

# -------------------------------------------------------------------
# Loan Status
# -------------------------------------------------------------------

RUNNING = "RUNNING"
COMPLETED = "COMPLETED"
DEFAULTED = "DEFAULTED"

# -------------------------------------------------------------------
# Transaction Types
# -------------------------------------------------------------------

DEPOSIT = "DEPOSIT"
WITHDRAWAL = "WITHDRAWAL"
TRANSFER = "TRANSFER"
UPI = "UPI"
ATM = "ATM"

# -------------------------------------------------------------------
# Common Boolean Flags
# -------------------------------------------------------------------

YES = "Y"
NO = "N"

# -------------------------------------------------------------------
# Date Formats
# -------------------------------------------------------------------

DATE_FORMAT = "yyyy-MM-dd"
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"

# -------------------------------------------------------------------
# Auto Loader
# -------------------------------------------------------------------

AUTO_LOADER = "cloudFiles"
AUTO_LOADER_SCHEMA_LOCATION = "_schemas"

# -------------------------------------------------------------------
# Monitoring
# -------------------------------------------------------------------

SUCCESS = "SUCCESS"
FAILED = "FAILED"
WARNING = "WARNING"

# -------------------------------------------------------------------
# Fraud Thresholds
# -------------------------------------------------------------------

HIGH_VALUE_TRANSACTION = 100000

FAILED_LOGIN_THRESHOLD = 5