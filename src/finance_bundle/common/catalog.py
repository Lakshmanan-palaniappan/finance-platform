from finance_bundle.common.config import settings


class Catalog:

    # ==========================================================
    # Internal helper
    # ==========================================================

    @staticmethod
    def _catalog(catalog=None):

        return catalog or settings.CATALOG

    # ==========================================================
    # Bronze
    # ==========================================================

    @staticmethod
    def bronze(table, catalog=None):

        catalog_name = Catalog._catalog(catalog)

        return (
            f"{catalog_name}."
            f"{settings.BRONZE_SCHEMA}."
            f"{table}"
        )

    # ==========================================================
    # Silver
    # ==========================================================

    @staticmethod
    def silver(table, catalog=None):

        catalog_name = Catalog._catalog(catalog)

        return (
            f"{catalog_name}."
            f"{settings.SILVER_SCHEMA}."
            f"{table}"
        )

    # ==========================================================
    # Gold
    # ==========================================================

    @staticmethod
    def gold(table, catalog=None):

        catalog_name = Catalog._catalog(catalog)

        return (
            f"{catalog_name}."
            f"{settings.GOLD_SCHEMA}."
            f"{table}"
        )

    # ==========================================================
    # Monitoring
    # ==========================================================

    @staticmethod
    def monitoring(table, catalog=None):

        catalog_name = Catalog._catalog(catalog)

        return (
            f"{catalog_name}."
            f"{settings.MONITORING_SCHEMA}."
            f"{table}"
        )

    # ==========================================================
    # Quarantine
    # ==========================================================

    @staticmethod
    def quarantine(table, catalog=None):

        catalog_name = Catalog._catalog(catalog)

        return (
            f"{catalog_name}."
            f"{settings.QUARANTINE_SCHEMA}."
            f"{table}"
        )