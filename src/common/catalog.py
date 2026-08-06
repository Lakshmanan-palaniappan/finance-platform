from common.config import settings


class Catalog:

    @staticmethod
    def bronze(table):

        return f"{settings.CATALOG}.{settings.BRONZE_SCHEMA}.{table}"

    @staticmethod
    def silver(table):

        return f"{settings.CATALOG}.{settings.SILVER_SCHEMA}.{table}"

    @staticmethod
    def gold(table):

        return f"{settings.CATALOG}.{settings.GOLD_SCHEMA}.{table}"

    @staticmethod
    def monitoring(table):

        return f"{settings.CATALOG}.{settings.MONITORING_SCHEMA}.{table}"
