from finance_bundle.common.config import settings

# ==========================================================
# Storage Root
# ==========================================================

ROOT = (
    f"abfss://{settings.CONTAINER}"
    f"@{settings.STORAGE_ACCOUNT}.dfs.core.windows.net"
)


# ==========================================================
# Landing Zone
# ==========================================================

class Landing:

    ROOT = f"{ROOT}/landing"

    MASTER = f"{ROOT}/master"
    STREAMING = f"{ROOT}/streaming"
    CDC = f"{ROOT}/cdc"


# ==========================================================
# Source Files
# ==========================================================

class Master:

    CUSTOMER = f"{Landing.MASTER}/customers"
    ACCOUNT = f"{Landing.MASTER}/accounts"
    CUSTOMER_KYC = f"{Landing.MASTER}/customer_kyc"
    LOAN = f"{Landing.MASTER}/loans"
    CARD = f"{Landing.MASTER}/cards"
    BRANCH = f"{Landing.MASTER}/branches"
    EXCHANGE_RATE = f"{Landing.MASTER}/exchange_rates"


class Streaming:

    TRANSACTION = f"{Landing.STREAMING}/transactions"
    ATM = f"{Landing.STREAMING}/atm_transactions"
    LOGIN = f"{Landing.STREAMING}/login_activity"


class CDC:

    CUSTOMER = f"{Landing.CDC}/customer_cdc"
    ACCOUNT = f"{Landing.CDC}/account_cdc"
    LOAN = f"{Landing.CDC}/loan_cdc"
    CARD = f"{Landing.CDC}/card_cdc"


# ==========================================================
# Bronze Storage
# ==========================================================

class Bronze:

    ROOT = f"{ROOT}/bronze"

    CUSTOMER = f"{ROOT}/customer"
    ACCOUNT = f"{ROOT}/account"
    LOAN = f"{ROOT}/loan"
    CARD = f"{ROOT}/card"
    TRANSACTION = f"{ROOT}/transaction"


# ==========================================================
# Silver Storage
# ==========================================================

class Silver:

    ROOT = f"{ROOT}/silver"

    CUSTOMER = f"{ROOT}/customer"
    ACCOUNT = f"{ROOT}/account"
    LOAN = f"{ROOT}/loan"


# ==========================================================
# Gold Storage
# ==========================================================

class Gold:

    ROOT = f"{ROOT}/gold"

    CUSTOMER360 = f"{ROOT}/customer360"
    DAILY_TRANSACTION = f"{ROOT}/daily_transaction_summary"
    BRANCH_PERFORMANCE = f"{ROOT}/branch_performance"


# ==========================================================
# Quarantine
# ==========================================================

class Quarantine:

    ROOT = f"{ROOT}/quarantine"

    CUSTOMER = f"{ROOT}/customer"
    ACCOUNT = f"{ROOT}/account"
    LOAN = f"{ROOT}/loan"


# ==========================================================
# Auto Loader Schema Location
# ==========================================================

class SchemaLocation:

    ROOT = f"{ROOT}/schema"

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


# ==========================================================
# Auto Loader Checkpoints
# ==========================================================

class Checkpoint:

    ROOT = f"{ROOT}/checkpoints"

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


# ==========================================================
# Backward Compatibility (Recommended for current code)
# ==========================================================

LOAN_INPUT_PATH = Master.LOAN
LOAN_SCHEMA_PATH = SchemaLocation.LOAN
LOAN_CHECKPOINT_PATH = Checkpoint.LOAN