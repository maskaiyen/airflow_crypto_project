from typing import Dict, List, TypedDict

from .constants import ValidationStatus


class ValidationItemReport(TypedDict, total=False):
    """
    Report structure for a single validation.
    """

    status: ValidationStatus
    total_rows: int
    failed_count: int
    failed_percentage: float
    examples: List[Dict]
    reason: str  # Reason when validation is skipped (e.g. "validation_not_run")


class ValidationSummary(TypedDict):
    """
    Aggregated summary for full data validation.
    """

    total: int
    executed: int
    passed: List[str]
    failed: List[str]
    skipped: List[str]


class ValidationReport(TypedDict):
    """
    Report structure for full data validation.
    """

    status: ValidationStatus
    stage: str
    total_rows: int
    validations: Dict[str, ValidationItemReport]
    summary: ValidationSummary


class SchemaErrorSummary(TypedDict):
    """
    Summary for failed schema validation.
    """

    passed: List[str]
    failed: List[str]


class SchemaErrorReport(TypedDict):
    """
    Report structure for failed schema validation.
    """

    status: ValidationStatus
    stage: str
    error: str
    error_message: str
    validations: Dict[str, Dict]
    summary: SchemaErrorSummary

