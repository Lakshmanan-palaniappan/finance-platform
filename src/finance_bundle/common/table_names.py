class Tables:
    # ==========================================================
    # Bronze
    # ==========================================================

    CUSTOMER = "customer"
    CUSTOMER_CDC = "customer_cdc"
    CUSTOMER_QUARANTINE = "customer_quarantine"

    ACCOUNT = "account"
    ACCOUNT_CDC = "account_cdc"
    ACCOUNT_QUARANTINE = "account_quarantine"

    CUSTOMER_KYC = "customer_kyc"
    CUSTOMER_KYC_QUARANTINE = "customer_kyc_quarantine"
    CUSTOMER_KYC_COMPLIANCE = "customer_kyc_compliance"

    LOAN = "loan"
    LOAN_CDC = "loan_cdc"
    LOAN_QUARANTINE = "loan_quarantine"

    CARD = "card"
    CARD_CDC = "card_cdc"
    CARD_QUARANTINE = "card_quarantine"

    BRANCH = "branch"
    EXCHANGE_RATE = "exchange_rate"
    TRANSACTION = "transaction"
    TRANSACTION_QUARANTINE = "transaction_quarantine"
    ATM_TRANSACTION = "atm_transaction"
    LOGIN_ACTIVITY = "login_activity"

    # ==========================================================
    # Gold
    # ==========================================================

    CUSTOMER360 = "customer360"
    CUSTOMER_RISK = "customer_risk"
    RELATIONSHIP_VALUE = "relationship_value"

    ACCOUNT_PORTFOLIO_SUMMARY = "account_portfolio_summary"
    ACCOUNT_BALANCE_SUMMARY = "account_balance_summary"

    DAILY_TRANSACTION_SUMMARY = "daily_transaction_summary"
    BRANCH_PERFORMANCE = "branch_performance"
    FRAUD_SUMMARY = "fraud_summary"
    ATM_SUMMARY = "atm_summary"
    EXECUTIVE_DASHBOARD = "executive_dashboard"
