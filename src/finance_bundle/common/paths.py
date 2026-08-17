from finance_bundle.common.config import settings


# ==========================================================
# Storage Root
# ==========================================================

STORAGE_ROOT = (
    f"abfss://{settings.CONTAINER}"
    f"@{settings.STORAGE_ACCOUNT}"
    f".dfs.core.windows.net"
)


# ==========================================================
# Landing Zone
# ==========================================================

class Landing:

    ROOT = f"{STORAGE_ROOT}/landing"

    MASTER = f"{ROOT}/master"
    STREAMING = f"{ROOT}/streaming"
    CDC = f"{ROOT}/cdc"


# ==========================================================
# Master Source Files
# ==========================================================

class Master:

    CUSTOMER = f"{Landing.MASTER}/customers"
    ACCOUNT = f"{Landing.MASTER}/accounts"
    CUSTOMER_KYC = f"{Landing.MASTER}/customer_kyc"
    LOAN = f"{Landing.MASTER}/loans"
    CARD = f"{Landing.MASTER}/cards"
    BRANCH = f"{Landing.MASTER}/branches"
    EXCHANGE_RATE = f"{Landing.MASTER}/exchange_rates"


# ==========================================================
# Streaming Source Files
# ==========================================================

class Streaming:

    TRANSACTION = (
        f"{Landing.STREAMING}/transactions"
    )

    ATM = (
        f"{Landing.STREAMING}/atm_transactions"
    )

    LOGIN = (
        f"{Landing.STREAMING}/login_activity"
    )


# ==========================================================
# CDC Source Files
# ==========================================================

class CDC:

    CUSTOMER = f"{Landing.CDC}/customer_cdc"
    ACCOUNT = f"{Landing.CDC}/account_cdc"
    LOAN = f"{Landing.CDC}/loan_cdc"
    CARD = f"{Landing.CDC}/card_cdc"


# ==========================================================
# Bronze
# ==========================================================

class Bronze:

    ROOT = f"{STORAGE_ROOT}/bronze"

    CUSTOMER = f"{ROOT}/customer"
    ACCOUNT = f"{ROOT}/account"
    LOAN = f"{ROOT}/loan"
    CARD = f"{ROOT}/card"
    TRANSACTION = f"{ROOT}/transaction"


# ==========================================================
# Silver
# ==========================================================

class Silver:

    ROOT = f"{STORAGE_ROOT}/silver"

    CUSTOMER = f"{ROOT}/customer"
    ACCOUNT = f"{ROOT}/account"
    LOAN = f"{ROOT}/loan"


# ==========================================================
# Gold
# ==========================================================

class Gold:

    ROOT = f"{STORAGE_ROOT}/gold"

    CUSTOMER360 = f"{ROOT}/customer360"

    ACCOUNT_PORTFOLIO_SUMMARY = (
        f"{ROOT}/account_portfolio_summary"
    )

    ACCOUNT_BALANCE_SUMMARY = (
        f"{ROOT}/account_balance_summary"
    )

    DAILY_TRANSACTION = (
        f"{ROOT}/daily_transaction_summary"
    )

    BRANCH_PERFORMANCE = (
        f"{ROOT}/branch_performance"
    )


# ==========================================================
# Quarantine
# ==========================================================

class Quarantine:

    ROOT = f"{STORAGE_ROOT}/quarantine"

    CUSTOMER = f"{ROOT}/customer"
    ACCOUNT = f"{ROOT}/account"
    LOAN = f"{ROOT}/loan"


# ==========================================================
# Auto Loader Schema Locations
# ==========================================================

class SchemaLocation:

    ROOT = f"{STORAGE_ROOT}/schema"

    CUSTOMER = f"{ROOT}/customer"
    CUSTOMER_CDC = f"{ROOT}/customer_cdc"

    ACCOUNT = f"{ROOT}/account"
    ACCOUNT_CDC = f"{ROOT}/account_cdc"

    CUSTOMER_KYC = f"{ROOT}/customer_kyc"
    LOAN = f"{ROOT}/loan"
    CARD = f"{ROOT}/card"
    BRANCH = f"{ROOT}/branch"
    EXCHANGE_RATE = f"{ROOT}/exchange_rate"
    TRANSACTION = f"{ROOT}/transaction"
    ATM = f"{ROOT}/atm_transaction"
    LOGIN = f"{ROOT}/login_activity"


# ==========================================================
# Checkpoints
# ==========================================================

class Checkpoint:

    ROOT = f"{STORAGE_ROOT}/checkpoints"

    # ------------------------------------------------------
    # Master / Bronze
    # ------------------------------------------------------

    CUSTOMER = f"{ROOT}/customer"
    ACCOUNT = f"{ROOT}/account"

    CUSTOMER_KYC = f"{ROOT}/customer_kyc"
    LOAN = f"{ROOT}/loan"
    CARD = f"{ROOT}/card"
    BRANCH = f"{ROOT}/branch"
    EXCHANGE_RATE = f"{ROOT}/exchange_rate"
    TRANSACTION = f"{ROOT}/transaction"
    ATM = f"{ROOT}/atm_transaction"
    LOGIN = f"{ROOT}/login_activity"

    # ------------------------------------------------------
    # CDC
    # ------------------------------------------------------

    CUSTOMER_CDC = f"{ROOT}/cdc/customer"
    ACCOUNT_CDC = f"{ROOT}/cdc/account"


# ==========================================================
# Customer Pipeline
# ==========================================================

CUSTOMER_INPUT_PATH = Master.CUSTOMER

CUSTOMER_SCHEMA_PATH = (
    SchemaLocation.CUSTOMER
)

CUSTOMER_CHECKPOINT_PATH = (
    Checkpoint.CUSTOMER
)


# ==========================================================
# Customer CDC Pipeline
# ==========================================================

CUSTOMER_CDC_INPUT_PATH = CDC.CUSTOMER

CUSTOMER_CDC_SCHEMA_PATH = (
    SchemaLocation.CUSTOMER_CDC
)

CUSTOMER_CDC_CHECKPOINT_PATH = (
    Checkpoint.CUSTOMER_CDC
)


# ==========================================================
# Account Pipeline
# ==========================================================

ACCOUNT_INPUT_PATH = Master.ACCOUNT

ACCOUNT_SCHEMA_PATH = (
    SchemaLocation.ACCOUNT
)

ACCOUNT_CHECKPOINT_PATH = (
    Checkpoint.ACCOUNT
)


# ==========================================================
# Account CDC Pipeline
# ==========================================================

ACCOUNT_CDC_INPUT_PATH = CDC.ACCOUNT

ACCOUNT_CDC_SCHEMA_PATH = (
    SchemaLocation.ACCOUNT_CDC
)

ACCOUNT_CDC_CHECKPOINT_PATH = (
    Checkpoint.ACCOUNT_CDC
)


# ==========================================================
# Loan Pipeline
# ==========================================================

LOAN_INPUT_PATH = Master.LOAN

LOAN_SCHEMA_PATH = (
    SchemaLocation.LOAN
)

LOAN_CHECKPOINT_PATH = (
    Checkpoint.LOAN
)


# ==========================================================
# Card Pipeline
# ==========================================================

CARD_INPUT_PATH = Master.CARD

CARD_SCHEMA_PATH = (
    SchemaLocation.CARD
)

CARD_CHECKPOINT_PATH = (
    Checkpoint.CARD
)


# ==========================================================
# Branch Pipeline
# ==========================================================

BRANCH_INPUT_PATH = Master.BRANCH

BRANCH_SCHEMA_PATH = (
    SchemaLocation.BRANCH
)

BRANCH_CHECKPOINT_PATH = (
    Checkpoint.BRANCH
)


# ==========================================================
# Exchange Rate Pipeline
# ==========================================================

EXCHANGE_RATE_INPUT_PATH = (
    Master.EXCHANGE_RATE
)

EXCHANGE_RATE_SCHEMA_PATH = (
    SchemaLocation.EXCHANGE_RATE
)

EXCHANGE_RATE_CHECKPOINT_PATH = (
    Checkpoint.EXCHANGE_RATE
)