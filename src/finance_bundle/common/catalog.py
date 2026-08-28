from finance_bundle.common.config import settings


class Catalog:

    # ==========================================================
    # Bronze
    # ==========================================================

    @staticmethod
    def bronze(table):

        return (
            f"{settings.BRONZE_SCHEMA}."
            f"{table}"
        )

    # ==========================================================
    # Silver
    # ==========================================================

    @staticmethod
    def silver(table):

        return (
            f"{settings.SILVER_SCHEMA}."
            f"{table}"
        )

    # ==========================================================
    # Gold
    # ==========================================================

    @staticmethod
    def gold(table):

        return (
            f"{settings.GOLD_SCHEMA}."
            f"{table}"
        )

    # ==========================================================
    # Monitoring
    # ==========================================================

    @staticmethod
    def monitoring(table):

        return (
            f"{settings.MONITORING_SCHEMA}."
            f"{table}"
        )

    # ==========================================================
    # Quarantine
    # ==========================================================

    @staticmethod
    def quarantine(table):

        return (
            f"{settings.QUARANTINE_SCHEMA}."
            f"{table}"
        )