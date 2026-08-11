from enum import Enum


class Layer(Enum):

    BRONZE = "bronze"

    SILVER = "silver"

    GOLD = "gold"


class Environment(Enum):

    DEV = "dev"

    QA = "qa"

    PROD = "prod"


class LoadType(Enum):

    FULL = "FULL"

    INCREMENTAL = "INCREMENTAL"


class PipelineType(Enum):

    MASTER = "master"

    STREAMING = "streaming"

    CDC = "cdc"


class Dataset(Enum):

    CUSTOMER = "customer"

    ACCOUNT = "account"

    LOAN = "loan"

    CARD = "card"

    CUSTOMER_KYC = "customer_kyc"

    BRANCH = "branch"

    EXCHANGE_RATE = "exchange_rate"

    TRANSACTION = "transaction"

    ATM_TRANSACTION = "atm_transaction"

    LOGIN_ACTIVITY = "login_activity"


class TransactionType(Enum):

    DEPOSIT = "DEPOSIT"

    WITHDRAWAL = "WITHDRAWAL"

    TRANSFER = "TRANSFER"

    UPI = "UPI"

    ATM = "ATM"


class Severity(Enum):

    INFO = "INFO"

    WARNING = "WARNING"

    ERROR = "ERROR"

    CRITICAL = "CRITICAL"