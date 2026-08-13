class Tables:

    # ==========================================================
    # Bronze / Silver domain tables
    # ==========================================================

    CUSTOMER = "customer"
    CUSTOMER_CDC = "customer_cdc"
    CUSTOMER_QUARANTINE = "customer_quarantine"

    ACCOUNT = "account"
    CUSTOMER_KYC = "customer_kyc"
    LOAN = "loan"
    CARD = "card"
    BRANCH = "branch"
    EXCHANGE_RATE = "exchange_rate"
    TRANSACTION = "transaction"
    ATM_TRANSACTION = "atm_transaction"
    LOGIN_ACTIVITY = "login_activity"

    # ==========================================================
    # Gold
    # ==========================================================

    CUSTOMER360 = "customer360"
    CUSTOMER_RISK = "customer_risk"
    RELATIONSHIP_VALUE = "relationship_value"

    DAILY_TRANSACTION_SUMMARY = "daily_transaction_summary"
    BRANCH_PERFORMANCE = "branch_performance"
    FRAUD_SUMMARY = "fraud_summary"
    ATM_SUMMARY = "atm_summary"