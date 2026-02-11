from enum import Enum

class SchemaValidationStatus(str, Enum):
    """
    Status codes for schema validation results.
    """

    VALID = "valid"
    EMPTY_DATA = "empty_data"
    INVALID_TYPE = "invalid_type"
    MISSING_REQUIRED_FIELDS = "missing_required_fields"


class ValidationStatus(str, Enum):
    """
    Overall status for the full validation run.
    """

    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"


class ValidationFields:
    """
    Field names for validation flags and metadata.
    """
    
    HAS_NON_NUMERIC_VALUE = "has_non_numeric_value"
    HAS_ABNORMAL_PRICE = "has_abnormal_price"
    HAS_INVALID_MARKET_CAP = "has_invalid_market_cap"
    HAS_MISSING_VALUES = "has_missing_values"
    HAS_DUPLICATE = "has_duplicate"
    VALIDATED_AT = "validated_at"