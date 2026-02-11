"""Unit tests for CryptoDataValidator."""

import pytest
import pandas as pd
from datetime import datetime

from src.validators import CryptoDataValidator
from src.constants import SchemaValidationStatus, ValidationStatus, ValidationFields


@pytest.fixture
def validator():
    """Create a validator instance."""
    return CryptoDataValidator()


@pytest.fixture
def valid_data():
    """Create valid test data with all required fields."""
    return pd.DataFrame({
        'id': ['bitcoin', 'ethereum', 'cardano'],
        'symbol': ['btc', 'eth', 'ada'],
        'name': ['Bitcoin', 'Ethereum', 'Cardano'],
        'current_price': [45000.0, 3000.0, 0.5],
        'market_cap': [900000000000, 360000000000, 16000000000],
        'total_volume': [30000000000, 15000000000, 500000000],
        'market_cap_rank': [1, 2, 10],
        'circulating_supply': [20000000, 120000000, 32000000000],
        'high_24h': [46000.0, 3100.0, 0.52],
        'low_24h': [44000.0, 2900.0, 0.48],
    })


class TestValidateSchema:
    """Test schema validation."""

    def test_valid_schema(self, validator, valid_data):
        """Test valid schema."""
        result = validator.validate_schema(valid_data)
        assert result == SchemaValidationStatus.VALID

    def test_empty_dataframe(self, validator):
        """Test empty DataFrame."""
        df = pd.DataFrame()
        result = validator.validate_schema(df)
        assert result == SchemaValidationStatus.EMPTY_DATA

    def test_none_input(self, validator):
        """Test None input."""
        result = validator.validate_schema(None)
        assert result == SchemaValidationStatus.EMPTY_DATA

    def test_invalid_type(self, validator):
        """Test non-DataFrame input."""
        result = validator.validate_schema("not a dataframe")
        assert result == SchemaValidationStatus.INVALID_TYPE

    def test_missing_required_fields(self, validator):
        """Test missing required fields."""
        df = pd.DataFrame({
            'id': ['bitcoin'],
            'symbol': ['btc'],
            'name': ['Bitcoin']
            # Missing: current_price, market_cap, total_volume
        })
        result = validator.validate_schema(df)
        assert result == SchemaValidationStatus.MISSING_REQUIRED_FIELDS


class TestFlagInvalidNumericTypes:
    """Test flag_invalid_numeric_types."""

    def test_all_valid_numeric(self, validator, valid_data):
        """Test all numeric fields are valid."""
        result = validator.flag_invalid_numeric_types(valid_data.copy())
        assert ValidationFields.HAS_NON_NUMERIC_VALUE in result.columns
        assert result[ValidationFields.HAS_NON_NUMERIC_VALUE].sum() == 0

    def test_invalid_numeric_string(self, validator):
        """Test non-numeric string in numeric field."""
        df = pd.DataFrame({
            'id': ['bitcoin'],
            'symbol': ['btc'],
            'name': ['Bitcoin'],
            'current_price': ['not_a_number'],
            'market_cap': [900000000000],
            'total_volume': [30000000000]
        })
        result = validator.flag_invalid_numeric_types(df)
        assert result[ValidationFields.HAS_NON_NUMERIC_VALUE].sum() == 1

    def test_mixed_valid_invalid(self, validator):
        """Test mixed valid and invalid numeric values."""
        df = pd.DataFrame({
            'id': ['bitcoin', 'ethereum'],
            'symbol': ['btc', 'eth'],
            'name': ['Bitcoin', 'Ethereum'],
            'current_price': [45000.0, 'invalid'],
            'market_cap': ['invalid', 360000000000],
            'total_volume': [30000000000, 15000000000]
        })
        result = validator.flag_invalid_numeric_types(df)
        assert result[ValidationFields.HAS_NON_NUMERIC_VALUE].sum() == 2


class TestFlagAbnormalPrices:
    """Test flag_abnormal_prices."""

    def test_valid_prices(self, validator, valid_data):
        """Test valid price range."""
        result = validator.flag_abnormal_prices(valid_data.copy())
        assert ValidationFields.HAS_ABNORMAL_PRICE in result.columns
        assert result[ValidationFields.HAS_ABNORMAL_PRICE].sum() == 0

    def test_price_too_low(self, validator):
        """Test price below minimum."""
        df = pd.DataFrame({
            'id': ['bitcoin'],
            'symbol': ['btc'],
            'name': ['Bitcoin'],
            'current_price': [0.0000001],  # Below 0.000001
            'market_cap': [900000000000],
            'total_volume': [30000000000]
        })
        result = validator.flag_abnormal_prices(df)
        assert result[ValidationFields.HAS_ABNORMAL_PRICE].sum() == 1

    def test_price_too_high(self, validator):
        """Test price above maximum."""
        df = pd.DataFrame({
            'id': ['bitcoin'],
            'symbol': ['btc'],
            'name': ['Bitcoin'],
            'current_price': [2000000],  # Above 1000000
            'market_cap': [900000000000],
            'total_volume': [30000000000]
        })
        result = validator.flag_abnormal_prices(df)
        assert result[ValidationFields.HAS_ABNORMAL_PRICE].sum() == 1

    def test_missing_price_column(self, validator):
        """Test missing price column doesn't crash."""
        df = pd.DataFrame({
            'id': ['bitcoin'],
            'symbol': ['btc'],
            'name': ['Bitcoin']
        })
        result = validator.flag_abnormal_prices(df)
        assert ValidationFields.HAS_ABNORMAL_PRICE in result.columns

    def test_with_invalid_string_type(self, validator):
        """Test that invalid string types don't cause crashes"""
        df = pd.DataFrame({
            'current_price': [45000.0, 'invalid', 2.5]
        })
        result = validator.flag_abnormal_prices(df)
        assert len(result) == 3
        assert result.loc[1, ValidationFields.HAS_ABNORMAL_PRICE] == False


class TestFlagInvalidMarketCap:
    """Test flag_invalid_market_cap."""

    def test_valid_market_cap(self, validator):
        """Test valid market cap calculation."""
        df = pd.DataFrame({
            'id': ['bitcoin'],
            'symbol': ['btc'],
            'name': ['Bitcoin'],
            'current_price': [100.0],
            'circulating_supply': [1000000.0],
            'market_cap': [100000000.0],  # 100 * 1000000 = 100000000
            'total_volume': [30000000000]
        })
        result = validator.flag_invalid_market_cap(df)
        assert ValidationFields.HAS_INVALID_MARKET_CAP in result.columns
        assert result[ValidationFields.HAS_INVALID_MARKET_CAP].sum() == 0

    def test_market_cap_within_tolerance(self, validator):
        """Test market cap within 5% tolerance."""
        df = pd.DataFrame({
            'id': ['bitcoin'],
            'symbol': ['btc'],
            'name': ['Bitcoin'],
            'current_price': [100.0],
            'circulating_supply': [1000000.0],
            'market_cap': [104000000.0],  # 4% error, within tolerance
            'total_volume': [30000000000]
        })
        result = validator.flag_invalid_market_cap(df)
        assert result[ValidationFields.HAS_INVALID_MARKET_CAP].sum() == 0

    def test_market_cap_exceeds_tolerance(self, validator):
        """Test market cap exceeds 5% tolerance."""
        df = pd.DataFrame({
            'id': ['bitcoin'],
            'symbol': ['btc'],
            'name': ['Bitcoin'],
            'current_price': [100.0],
            'circulating_supply': [1000000.0],
            'market_cap': [110000000.0],  # 10% error, exceeds tolerance
            'total_volume': [30000000000]
        })
        result = validator.flag_invalid_market_cap(df)
        assert result[ValidationFields.HAS_INVALID_MARKET_CAP].sum() == 1

    def test_negative_market_cap(self, validator):
        """Test negative market cap is flagged."""
        df = pd.DataFrame({
            'id': ['bitcoin'],
            'symbol': ['btc'],
            'name': ['Bitcoin'],
            'current_price': [100.0],
            'circulating_supply': [1000000.0],
            'market_cap': [-100000000.0],
            'total_volume': [30000000000]
        })
        result = validator.flag_invalid_market_cap(df)
        assert result[ValidationFields.HAS_INVALID_MARKET_CAP].sum() == 1

    def test_zero_market_cap(self, validator):
        """Test zero market cap is flagged."""
        df = pd.DataFrame({
            'id': ['bitcoin'],
            'symbol': ['btc'],
            'name': ['Bitcoin'],
            'current_price': [100.0],
            'circulating_supply': [1000000.0],
            'market_cap': [0.0],
            'total_volume': [30000000000]
        })
        result = validator.flag_invalid_market_cap(df)
        assert result[ValidationFields.HAS_INVALID_MARKET_CAP].sum() == 1

    def test_missing_required_columns(self, validator):
        """Test missing required columns."""
        df = pd.DataFrame({
            'id': ['bitcoin'],
            'symbol': ['btc'],
            'name': ['Bitcoin'],
            'current_price': [100.0]
            # Missing market_cap and circulating_supply
        })
        result = validator.flag_invalid_market_cap(df)
        assert ValidationFields.HAS_INVALID_MARKET_CAP in result.columns
        assert result[ValidationFields.HAS_INVALID_MARKET_CAP].sum() == 0

    def test_with_invalid_types_in_calculation(self, validator):
        """Test that invalid types in any field don't crash calculation"""
        df = pd.DataFrame({
            'market_cap': [900000000000, 'invalid', 50000000000],
            'current_price': [45000.0, 2500.0, 'invalid'],
            'circulating_supply': [20000000, 'invalid', 25000000000]
        })
        result = validator.flag_invalid_market_cap(df)
        assert len(result) == 3


class TestFlagMissingValues:
    """Test flag_missing_values."""

    def test_no_missing_values(self, validator, valid_data):
        """Test no missing values."""
        result = validator.flag_missing_values(valid_data.copy())
        assert ValidationFields.HAS_MISSING_VALUES in result.columns
        assert result[ValidationFields.HAS_MISSING_VALUES].sum() == 0

    def test_single_missing_value(self, validator):
        """Test single missing value."""
        df = pd.DataFrame({
            'id': ['bitcoin', 'ethereum'],
            'symbol': ['btc', None],
            'name': ['Bitcoin', 'Ethereum'],
            'current_price': [45000.0, 3000.0],
            'market_cap': [900000000000, 360000000000],
            'circulating_supply': [20000000, 120000000],
            'total_volume': [30000000000, 15000000000]
        })
        result = validator.flag_missing_values(df)
        assert result[ValidationFields.HAS_MISSING_VALUES].sum() == 1
        assert result.loc[1, ValidationFields.HAS_MISSING_VALUES] == True


class TestFlagDuplicates:
    """Test flag_duplicates."""

    def test_no_duplicates(self, validator, valid_data):
        """Test no duplicates."""
        result = validator.flag_duplicates(valid_data.copy())
        assert ValidationFields.HAS_DUPLICATE in result.columns
        assert result[ValidationFields.HAS_DUPLICATE].sum() == 0

    def test_with_duplicates(self, validator):
        """Test with duplicates."""
        df = pd.DataFrame({
            'id': ['bitcoin', 'bitcoin', 'ethereum'],
            'symbol': ['btc', 'btc', 'eth'],
            'name': ['Bitcoin', 'Bitcoin', 'Ethereum'],
            'current_price': [45000.0, 45000.0, 3000.0],
            'market_cap': [900000000000, 900000000000, 360000000000],
            'total_volume': [30000000000, 30000000000, 15000000000]
        })
        result = validator.flag_duplicates(df)
        assert result[ValidationFields.HAS_DUPLICATE].sum() == 1
        assert result.loc[1, ValidationFields.HAS_DUPLICATE] == True

    def test_missing_id_column(self, validator):
        """Test missing id column."""
        df = pd.DataFrame({
            'symbol': ['btc', 'eth'],
            'name': ['Bitcoin', 'Ethereum']
        })
        result = validator.flag_duplicates(df)
        assert ValidationFields.HAS_DUPLICATE in result.columns
        assert result[ValidationFields.HAS_DUPLICATE].sum() == 0


class TestAddMetadata:
    """Test add_metadata."""

    def test_adds_metadata_column(self, validator, valid_data):
        """Test metadata column is added."""
        result = validator.add_metadata(valid_data.copy())
        assert ValidationFields.VALIDATED_AT in result.columns
        assert result[ValidationFields.VALIDATED_AT].notna().all()


class TestGenerateSchemaErrorReport:
    """Test generate_schema_error_report."""

    def test_empty_data_report(self, validator):
        """Test report for empty data."""
        report = validator.generate_schema_error_report(SchemaValidationStatus.EMPTY_DATA)
        assert report['status'] == ValidationStatus.FAILED
        assert report['stage'] == 'schema_validation'
        assert report['error'] == 'empty_data'
        assert report['error_message'] == "DataFrame is empty or None"

    def test_invalid_type_report(self, validator):
        """Test report for invalid type."""
        report = validator.generate_schema_error_report(SchemaValidationStatus.INVALID_TYPE)
        assert report['status'] == ValidationStatus.FAILED
        assert report['error'] == 'invalid_type'

    def test_missing_fields_report(self, validator):
        """Test report for missing required fields."""
        report = validator.generate_schema_error_report(SchemaValidationStatus.MISSING_REQUIRED_FIELDS)
        assert report['status'] == ValidationStatus.FAILED
        assert report['error'] == 'missing_required_fields'


class TestGenerateValidationReport:
    """Test generate_validation_report."""

    def test_report_with_all_flags(self, validator, valid_data):
        """Test report generation with all flag columns."""
        df = valid_data.copy()
        df = validator.flag_invalid_numeric_types(df)
        df = validator.flag_abnormal_prices(df)
        df = validator.flag_invalid_market_cap(df)
        df = validator.flag_missing_values(df)
        df = validator.flag_duplicates(df)

        report = validator.generate_validation_report(df)

        assert report['status'] == ValidationStatus.PASSED
        assert report['stage'] == 'data_validation'
        assert report['total_rows'] == len(df)
        assert 'validations' in report
        assert 'summary' in report
        assert len(report['validations']) == 5

    def test_report_with_failures(self, validator):
        """Test report with validation failures."""
        df = pd.DataFrame({
            'id': ['bitcoin', 'bitcoin'],
            'symbol': ['btc', 'btc'],
            'name': ['Bitcoin', 'Bitcoin'],
            'current_price': [45000.0, 'invalid'],
            'market_cap': [900000000000, 360000000000],
            'circulating_supply': [20000000, 120000000],
            'total_volume': [30000000000, 15000000000]
        })

        df = validator.flag_invalid_numeric_types(df)
        df = validator.flag_abnormal_prices(df)
        df = validator.flag_invalid_market_cap(df)
        df = validator.flag_missing_values(df)
        df = validator.flag_duplicates(df)

        report = validator.generate_validation_report(df)
        assert report['status'] == ValidationStatus.FAILED
        assert len(report['summary']['failed']) > 0


    def test_report_without_flags(self, validator, valid_data):
        """Test report generation without flag columns."""
        # Don't run any flag methods
        report = validator.generate_validation_report(valid_data.copy())

        # All validations should be skipped
        for validation in report['validations'].values():
            assert validation['status'] == ValidationStatus.SKIPPED


class TestGenerateFlagReport:
    """Test _generate_flag_report (private method)."""

    def test_skipped_when_flag_missing(self, validator, valid_data):
        """Test skipped status when flag column is missing."""
        report = validator._generate_flag_report(
            valid_data.copy(),
            'non_existent_flag',
            'test_validation'
        )
        assert report['status'] == ValidationStatus.SKIPPED
        assert report['reason'] == 'validation_not_run'

    def test_passed_when_no_failures(self, validator, valid_data):
        """Test passed status when no failures."""
        df = valid_data.copy()
        df[ValidationFields.HAS_DUPLICATE] = False

        report = validator._generate_flag_report(
            df,
            ValidationFields.HAS_DUPLICATE,
            'duplicates'
        )
        assert report['status'] == ValidationStatus.PASSED
        assert report['failed_count'] == 0

    def test_failed_with_examples(self, validator):
        """Test failed status with example rows."""
        df = pd.DataFrame({
            'id': ['bitcoin', 'ethereum'],
            'symbol': ['btc', 'eth'],
            'name': ['Bitcoin', 'Ethereum'],
            'current_price': [45000.0, 3000.0],
            'market_cap': [900000000000, 360000000000],
            'total_volume': [30000000000, 15000000000]
        })
        df[ValidationFields.HAS_DUPLICATE] = [False, True]

        report = validator._generate_flag_report(
            df,
            ValidationFields.HAS_DUPLICATE,
            'duplicates',
            example_columns=['id', 'symbol', 'name']
        )
        assert report['status'] == ValidationStatus.FAILED
        assert report['failed_count'] == 1
        assert 'examples' in report
        assert len(report['examples']) == 1


class TestIntegration:
    """Integration tests for full validation pipeline."""

    def test_full_pipeline_clean_data(self, validator, valid_data):
        """Test full validation pipeline with clean data."""
        df = valid_data.copy()

        # Schema validation
        schema_result = validator.validate_schema(df)
        assert schema_result == SchemaValidationStatus.VALID

        # Run all flag methods
        df = validator.flag_invalid_numeric_types(df)
        df = validator.flag_abnormal_prices(df)
        df = validator.flag_invalid_market_cap(df)
        df = validator.flag_missing_values(df)
        df = validator.flag_duplicates(df)
        df = validator.add_metadata(df)

        # Generate report
        report = validator.generate_validation_report(df)

        assert report['status'] == ValidationStatus.PASSED
        assert all(
            v['status'] == ValidationStatus.PASSED
            for v in report['validations'].values()
        )

    def test_full_pipeline_dirty_data(self, validator):
        """Test full validation pipeline with dirty data."""
        df = pd.DataFrame({
            'id': ['bitcoin', 'bitcoin', 'ethereum', None],
            'symbol': ['btc', 'btc', 'eth', 'ada'],
            'name': ['Bitcoin', 'Bitcoin', 'Ethereum', 'Cardano'],
            'current_price': [45000.0, 'invalid', -100.0, 0.5],
            'market_cap': [900000000000, 900000000000, 360000000000, 16000000000],
            'total_volume': [30000000000, 30000000000, 15000000000, 500000000],
            'circulating_supply': [19000000, 19000000, 120000000, 35000000000]
        })
        
        # Schema validation should pass (has all required fields)
        schema_result = validator.validate_schema(df)
        assert schema_result == SchemaValidationStatus.VALID

        # Run all flag methods
        df = validator.flag_invalid_numeric_types(df)
        df = validator.flag_abnormal_prices(df)
        df = validator.flag_invalid_market_cap(df)
        df = validator.flag_missing_values(df)
        df = validator.flag_duplicates(df)

        # Should detect issues
        assert df[ValidationFields.HAS_NON_NUMERIC_VALUE].sum() > 0
        assert df[ValidationFields.HAS_ABNORMAL_PRICE].sum() > 0
        assert df[ValidationFields.HAS_MISSING_VALUES].sum() > 0
        assert df[ValidationFields.HAS_DUPLICATE].sum() > 0

        # Generate report
        report = validator.generate_validation_report(df)
        assert report['status'] == ValidationStatus.FAILED
        assert len(report['summary']['failed']) > 0


