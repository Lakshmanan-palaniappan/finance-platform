from dataclasses import dataclass



class Settings:

    PROJECT_NAME = "enterprise-banking-platform"

    STORAGE_ACCOUNT = "bankingstorageacc"

    CONTAINER = "finance-dev"

    CATALOG = "finance_catalog"

    EXTERNAL_LOCATION = "storage-connector"

    BRONZE_SCHEMA = "bronze"

    SILVER_SCHEMA = "silver"

    GOLD_SCHEMA = "gold"

    MONITORING_SCHEMA = "monitoring"

    QUARANTINE_SCHEMA = "quarantine"

    FILE_FORMAT = "csv"

    HEADER = True

    SCHEMA_EVOLUTION = "addNewColumns"

    AUTOLOADER = "cloudFiles"

    TIMEZONE = "Asia/Kolkata"


settings = Settings()