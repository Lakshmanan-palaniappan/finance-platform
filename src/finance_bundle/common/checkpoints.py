from finance_bundle.common.paths import Checkpoint


class Checkpoints:

    ROOT = Checkpoint.ROOT


class BronzeCheckpoint:

    CUSTOMER = f"{Checkpoints.ROOT}/bronze/customer"
    ACCOUNT = f"{Checkpoints.ROOT}/bronze/account"
    CUSTOMER_KYC = f"{Checkpoints.ROOT}/bronze/customer_kyc"
    LOAN = f"{Checkpoints.ROOT}/bronze/loan"
    CARD = f"{Checkpoints.ROOT}/bronze/card"
    BRANCH = f"{Checkpoints.ROOT}/bronze/branch"
    EXCHANGE_RATE = f"{Checkpoints.ROOT}/bronze/exchange_rate"
    TRANSACTION = f"{Checkpoints.ROOT}/bronze/transaction"
    ATM_TRANSACTION = f"{Checkpoints.ROOT}/bronze/atm_transaction"
    LOGIN_ACTIVITY = f"{Checkpoints.ROOT}/bronze/login_activity"


class CDCCheckpoint:

    CUSTOMER = Checkpoint.CUSTOMER_CDC


class SilverCheckpoint:

    CUSTOMER = f"{Checkpoints.ROOT}/silver/customer"
    ACCOUNT = f"{Checkpoints.ROOT}/silver/account"
    CUSTOMER_KYC = f"{Checkpoints.ROOT}/silver/customer_kyc"
    LOAN = f"{Checkpoints.ROOT}/silver/loan"
    CARD = f"{Checkpoints.ROOT}/silver/card"
    BRANCH = f"{Checkpoints.ROOT}/silver/branch"
    EXCHANGE_RATE = f"{Checkpoints.ROOT}/silver/exchange_rate"
    TRANSACTION = f"{Checkpoints.ROOT}/silver/transaction"
    ATM_TRANSACTION = f"{Checkpoints.ROOT}/silver/atm_transaction"
    LOGIN_ACTIVITY = f"{Checkpoints.ROOT}/silver/login_activity"


class GoldCheckpoint:

    CUSTOMER360 = f"{Checkpoints.ROOT}/gold/customer360"
    DAILY_TRANSACTION_SUMMARY = (
        f"{Checkpoints.ROOT}/gold/daily_transaction_summary"
    )
    BRANCH_PERFORMANCE = (
        f"{Checkpoints.ROOT}/gold/branch_performance"
    )
    FRAUD_SUMMARY = (
        f"{Checkpoints.ROOT}/gold/fraud_summary"
    )
    ATM_SUMMARY = (
        f"{Checkpoints.ROOT}/gold/atm_summary"
    )
    EXECUTIVE_DASHBOARD = (
        f"{Checkpoints.ROOT}/gold/executive_dashboard"
    )