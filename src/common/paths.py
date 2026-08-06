from common.config import settings


ROOT = (
    f"abfss://{settings.CONTAINER}"
    f"@{settings.STORAGE_ACCOUNT}.dfs.core.windows.net"
)
class Landing:

    MASTER = f"{ROOT}/landing/master"

    STREAMING = f"{ROOT}/landing/streaming"

    CDC = f"{ROOT}/landing/cdc"


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

class Bronze:

    ROOT = f"{ROOT}/bronze"

    CUSTOMER = f"{ROOT}/bronze/customer"

    ACCOUNT = f"{ROOT}/bronze/account"

    LOAN = f"{ROOT}/bronze/loan"

    CARD = f"{ROOT}/bronze/card"

    TRANSACTION = f"{ROOT}/bronze/transaction"

class Silver:

    ROOT = f"{ROOT}/silver"

    CUSTOMER = f"{ROOT}/silver/customer"

    ACCOUNT = f"{ROOT}/silver/account"

    LOAN = f"{ROOT}/silver/loan"
    
class Gold:

    ROOT = f"{ROOT}/gold"

    CUSTOMER360 = f"{ROOT}/gold/customer360"

    DAILY_TRANSACTION = f"{ROOT}/gold/daily_transaction_summary"

    BRANCH_PERFORMANCE = f"{ROOT}/gold/branch_performance"

class Quarantine:

    ROOT = f"{ROOT}/quarantine"

    CUSTOMER = f"{ROOT}/quarantine/customer"

    ACCOUNT = f"{ROOT}/quarantine/account"

    LOAN = f"{ROOT}/quarantine/loan"