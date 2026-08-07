"""
Custom exceptions used across the project.
"""


class BankingPipelineException(Exception):
    """Base exception for the project."""


class ConfigurationException(BankingPipelineException):
    """Raised when configuration is invalid."""


class SchemaValidationException(BankingPipelineException):
    """Raised when schema validation fails."""


class DataQualityException(BankingPipelineException):
    """Raised when expectations fail."""


class AutoLoaderException(BankingPipelineException):
    """Raised when Auto Loader fails."""


class DeltaMergeException(BankingPipelineException):
    """Raised when Delta MERGE fails."""


class CDCException(BankingPipelineException):
    """Raised during CDC processing."""


class SCDException(BankingPipelineException):
    """Raised during SCD Type 2 processing."""


class CheckpointException(BankingPipelineException):
    """Raised when checkpoint path is invalid."""


class FileMoveException(BankingPipelineException):
    """Raised when archive/quarantine move fails."""


class QuarantineException(BankingPipelineException):
    """Raised while writing quarantine records."""


class MonitoringException(BankingPipelineException):
    """Raised while writing monitoring metrics."""