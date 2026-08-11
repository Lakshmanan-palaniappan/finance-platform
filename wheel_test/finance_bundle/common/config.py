class Settings:

    PROJECT_NAME = "enterprise-banking-platform"

    # ==========================================================
    # Storage
    # ==========================================================

    STORAGE_ACCOUNT = "bankingstorageacc"
    CONTAINER = "finance-dev"

    # ==========================================================
    # Unity Catalog
    # ==========================================================

    CATALOG = "finance_catalog"

    EXTERNAL_LOCATION = "storage-connector"

    BRONZE_SCHEMA = "bronze"
    SILVER_SCHEMA = "silver"
    GOLD_SCHEMA = "gold"
    MONITORING_SCHEMA = "monitoring"
    QUARANTINE_SCHEMA = "quarantine"

    # ==========================================================
    # Auto Loader
    # ==========================================================

    FILE_FORMAT = "csv"
    HEADER = True
    AUTOLOADER = "cloudFiles"

    # IMPORTANT:
    # Do NOT use .schema(loan_schema) together with this.
    SCHEMA_EVOLUTION = "addNewColumns"

    # ==========================================================
    # Timezone
    # ==========================================================

    TIMEZONE = "Asia/Kolkata"


settings = Settings()