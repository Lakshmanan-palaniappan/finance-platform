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
    # The catalog is NOT defined here.
    #
    # The Databricks Bundle / Lakeflow pipeline determines
    # the catalog through:
    #
    #     catalog: ${var.catalog}
    #
    # Therefore:
    #
    #     dev-leo  -> finance_catalog_leo
    #     dev-aish -> finance_catalog_alj
    #     prod     -> finance_catalog
    #
    # Python only needs the schema names.
    # ==========================================================

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