from finance_bundle.common.config import settings


class Catalog:

    @staticmethod
    def bronze(table):

        return (
            f"{settings.CATALOG}."
            f"{settings.BRONZE_SCHEMA}."
            f"{table}"
        )

    @staticmethod
    def silver(table):

        return (
            f"{settings.CATALOG}."
            f"{settings.SILVER_SCHEMA}."
            f"{table}"
        )

    @staticmethod
    def gold(table):

        return (
            f"{settings.CATALOG}."
            f"{settings.GOLD_SCHEMA}."
            f"{table}"
        )

    @staticmethod
    def monitoring(table):

        return (
            f"{settings.CATALOG}."
            f"{settings.MONITORING_SCHEMA}."
            f"{table}"
        )

    @staticmethod
    def quarantine(table):

        return (
            f"{settings.CATALOG}."
            f"{settings.QUARANTINE_SCHEMA}."
            f"{table}"
        )