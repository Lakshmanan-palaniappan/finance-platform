import os


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
    #
    # The Bundle target can provide FINANCE_CATALOG.
    #
    # Example:
    #
    #   dev-leo    -> finance_catalog_leo
    #   dev-aish   -> finance_catalog_alj
    #   prod       -> finance_catalog
    #
    # If FINANCE_CATALOG is not provided, production catalog
    # is used as the safe fallback.
    # ==========================================================

    CATALOG = os.getenv(
        "FINANCE_CATALOG",
        "finance_catalog",
    )

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
    # Do NOT use .schema(<explicit_schema>) together with this
    # unless the particular ingestion implementation requires it.
    SCHEMA_EVOLUTION = "addNewColumns"

    # ==========================================================
    # Timezone
    # ==========================================================

    TIMEZONE = "Asia/Kolkata"


settings = Settings()