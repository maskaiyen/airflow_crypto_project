import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
import logging
from .constants import ValidationFields, SchemaValidationStatus, ValidationStatus
from .report_types import (
    ValidationItemReport,
    ValidationSummary,
    ValidationReport,
    SchemaErrorSummary,
    SchemaErrorReport,
)

logger = logging.getLogger('crypto_data_validator')


class CryptoDataValidator:
    """Crypto market data validator"""
    
    # Required field definitions
    REQUIRED_FIELDS = [
        'id', 'symbol', 'name', 'current_price', 
        'market_cap', 'total_volume', 'circulating_supply'
    ]
    
    # Numeric field definitions
    NUMERIC_FIELDS = [
        'current_price', 'market_cap', 'market_cap_rank',
        'total_volume', 'high_24h', 'low_24h',
        'price_change_24h', 'price_change_percentage_24h',
        'market_cap_change_24h', 'market_cap_change_percentage_24h',
        'circulating_supply', 'total_supply', 'max_supply'
    ]
    
    # Reasonable price range (USD)
    PRICE_RANGE = {
        'min': 0.000001,  # Minimum price
        'max': 1000000    # Maximum price
    }


    def validate_schema(self, df: pd.DataFrame) -> SchemaValidationStatus:
        """
        Validate data structure

        Returns:
            SchemaValidationStatus: status
        """
        
        # Check if empty
        if df is None:
            logger.error("Schema validation failed: DataFrame is None")
            return SchemaValidationStatus.EMPTY_DATA
        
        # Check if DataFrame
        if not isinstance(df, pd.DataFrame):
            logger.error(f"Schema validation failed: expected DataFrame, got {type(df)}")
            return SchemaValidationStatus.INVALID_TYPE
        
        if df.empty:
            logger.warning("Schema validation failed: DataFrame is empty")
            return SchemaValidationStatus.EMPTY_DATA
        
        # Check for missing fields
        missing_fields = set(self.REQUIRED_FIELDS) - set(df.columns)
        if missing_fields:
            logger.warning(f"Missing required fields: {missing_fields}")
            return SchemaValidationStatus.MISSING_REQUIRED_FIELDS
        
        logger.info("Schema validation passed")
        return SchemaValidationStatus.VALID
    

    def flag_invalid_numeric_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Flag rows with non-numeric values in numeric fields

        Returns:
            DataFrame with has_non_numeric_value column added
        """
            
        # Initialize flag column
        df[ValidationFields.HAS_NON_NUMERIC_VALUE] = False
        
        for field in self.NUMERIC_FIELDS:
            if field in df.columns:
                invalid_mask = pd.to_numeric(df[field], errors='coerce').isna() & df[field].notna()
                df.loc[invalid_mask, ValidationFields.HAS_NON_NUMERIC_VALUE] = True
        
        total_invalid = df[ValidationFields.HAS_NON_NUMERIC_VALUE].sum()
        if total_invalid > 0:
            logger.warning(f"Found {total_invalid} rows with non-numeric values")
        
        logger.info("Numeric type validation completed")
        return df
    
    
    def flag_abnormal_prices(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Flag rows where current_price is not within the acceptable range
        (0.000001 - 1,000,000 USD) or is null.
        
        Returns:
            DataFrame with has_abnormal_price column added
        """
        
        # Initialize flag column
        df[ValidationFields.HAS_ABNORMAL_PRICE] = False

        if 'current_price' in df.columns:
            numeric_prices = pd.to_numeric(df['current_price'], errors='coerce')
            abnormal_price = (
                numeric_prices.notna() & (
                    (numeric_prices < self.PRICE_RANGE['min']) |
                    (numeric_prices > self.PRICE_RANGE['max'])
                )
            )

            df.loc[abnormal_price, ValidationFields.HAS_ABNORMAL_PRICE] = True

            abnormal_count = df[ValidationFields.HAS_ABNORMAL_PRICE].sum()
            if abnormal_count > 0:
                logger.warning(f"Found {abnormal_count} abnormal prices")
        
        logger.info("Price range validation completed")
        return df
    

    def flag_invalid_market_cap(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Flag rows with invalid market cap values
        
        Validates that market_cap ≈ current_price × circulating_supply
        (within 5% tolerance). Marks rows as invalid if:
        - Market cap is negative or zero
        - Calculation error exceeds 5%
        - Required fields are missing (marked as False, not invalid)
        
        Returns:
            DataFrame with has_invalid_market_cap column added
        """
        
        # Initialize flag column
        df[ValidationFields.HAS_INVALID_MARKET_CAP] = False
        
        if all(col in df.columns for col in ['market_cap', 'current_price', 'circulating_supply']):
            numeric_market_cap = pd.to_numeric(df['market_cap'], errors='coerce')
            numeric_price = pd.to_numeric(df['current_price'], errors='coerce')
            numeric_supply = pd.to_numeric(df['circulating_supply'], errors='coerce')
            
            # Only validate rows with complete data
            can_validate = (
                numeric_market_cap.notna() & 
                numeric_price.notna() &
                numeric_supply.notna()
            )
            
            if can_validate.any():
                calculated_market_cap = numeric_price * numeric_supply
                
                # Mark negative or zero market cap as invalid
                negative_cap = can_validate & (numeric_market_cap <= 0)
                
                # Compute relative error (excluding non-positive market cap)
                valid_for_calc = can_validate & (numeric_market_cap > 0)
                relative_error = abs(numeric_market_cap - calculated_market_cap) / numeric_market_cap
                error_too_large = valid_for_calc & (relative_error >= 0.05)
                
                # Mark rows with invalid market cap
                df.loc[negative_cap | error_too_large, ValidationFields.HAS_INVALID_MARKET_CAP] = True
                
                # Log invalid count
                invalid_count = df[ValidationFields.HAS_INVALID_MARKET_CAP].sum()
                if invalid_count > 0:
                    logger.warning(f"Found {invalid_count} invalid market cap entries")
            else:
                logger.warning("No rows have complete data for market cap validation")
        
        logger.info("Market cap validation completed")
        return df
    

    def flag_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Flag rows with missing values in required fields.

        Returns:
            DataFrame with has_missing_values column added.
        """
        
        # Initialize flag column
        df[ValidationFields.HAS_MISSING_VALUES] = False
        
        # Flag rows with missing values
        has_missing = df[self.REQUIRED_FIELDS].isnull().any(axis=1)
        df.loc[has_missing, ValidationFields.HAS_MISSING_VALUES] = True
        missing_count = df[ValidationFields.HAS_MISSING_VALUES].sum()
        
        if missing_count > 0:
            logger.warning(f"Found {missing_count} records with missing values")
        
        logger.info("Missing value validation completed")
        return df
    
    
    def flag_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Flag rows with duplicate values based on 'id' field
        
        Returns:
            DataFrame with has_duplicates column added
        """
        
        # Initialize flag column
        df[ValidationFields.HAS_DUPLICATE] = False
        
        if 'id' in df.columns:
            has_duplicate = df.duplicated(subset=['id'], keep='first')
            df.loc[has_duplicate, ValidationFields.HAS_DUPLICATE] = True
            duplicate_count = df[ValidationFields.HAS_DUPLICATE].sum()

            if duplicate_count > 0:
                logger.warning(f"Found {duplicate_count} duplicate records")
        
        logger.info("Duplicate validation completed")
        return df
    
    
    def add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add validation metadata columns

        Returns:
            DataFrame with validated_at column added
        """
        
        df[ValidationFields.VALIDATED_AT] = datetime.now(ZoneInfo("Asia/Taipei")).isoformat()
        return df
    
    
    def generate_schema_error_report(self, status: SchemaValidationStatus) -> SchemaErrorReport:
        """
        Build a structured report for schema validation failures.

        Args:
            status: Result of schema validation.
        """

        error_messages = {
            SchemaValidationStatus.EMPTY_DATA: "DataFrame is empty or None",
            SchemaValidationStatus.INVALID_TYPE: "Input is not a valid DataFrame",
            SchemaValidationStatus.MISSING_REQUIRED_FIELDS: "Missing required fields"
        }
        
        return {
            'status': ValidationStatus.FAILED,
            'stage': 'schema_validation',
            'error': status.value,
            'error_message': error_messages.get(status, "Unknown schema error"),
            'validations': {},
            'summary': {
                'passed': [],
                'failed': ['schema_validation']
            }
        }
    
    
    def generate_validation_report(self, df: pd.DataFrame) -> ValidationReport:
        """
        Build a full validation report from a flagged DataFrame.

        Args:
            df: DataFrame with validation flag columns.
        """
        
        report = {
            'status': ValidationStatus.PASSED,
            'stage': 'data_validation',
            'total_rows': len(df),
            'validations': {},
            'summary': {
                'total': 0,
                'executed': 0,
                'passed': [],
                'failed': [],
                'skipped': [] 
            }
        }
        
        # Validation reports
        report['validations']['numeric_types'] = self._generate_flag_report(
            df, ValidationFields.HAS_NON_NUMERIC_VALUE, 'numeric_types',
            example_columns=['symbol', 'name']
        )
        
        report['validations']['price_range'] = self._generate_flag_report(
            df, ValidationFields.HAS_ABNORMAL_PRICE, 'price_range',
            example_columns=['symbol', 'name', 'current_price']
        )
        
        report['validations']['market_cap'] = self._generate_flag_report(
            df, ValidationFields.HAS_INVALID_MARKET_CAP, 'market_cap',
            example_columns=['symbol', 'name', 'market_cap', 'current_price', 'circulating_supply']
        )

        report['validations']['missing_values'] = self._generate_flag_report(
            df, ValidationFields.HAS_MISSING_VALUES, 'missing_values',
            example_columns=['symbol', 'name']
        )
        
        report['validations']['duplicates'] = self._generate_flag_report(
            df, ValidationFields.HAS_DUPLICATE, 'duplicates',
            example_columns=['id', 'symbol', 'name']
        )
        
        report['summary']['total'] = len(report['validations'])

        # Update summary
        for key, validation in report['validations'].items():
            status = validation['status']
            if status == ValidationStatus.PASSED:
                report['summary'][ValidationStatus.PASSED].append(key)
                report['summary']['executed'] += 1
            elif status == ValidationStatus.FAILED:
                report['summary'][ValidationStatus.FAILED].append(key)
                report['summary']['executed'] += 1
            elif status == ValidationStatus.SKIPPED:
                report['summary']['skipped'].append(key)
        
        # Overall validation status
        if report['summary']['failed']:
            report['status'] = ValidationStatus.FAILED
        else:
            report['status'] = ValidationStatus.PASSED
        
        return report
    
    
    def _generate_flag_report(self, df: pd.DataFrame, flag_column: str, validation_name: str, example_columns: list = None) -> ValidationItemReport:
        """
        Build a report object for a single validation flag column.

        Args:
            df: Input DataFrame.
            flag_column: Boolean flag column indicating failures.
            validation_name: Logical name of the validation.
            example_columns: Optional columns to include as examples.
        """
        
        # If the flag column is missing, the validation was not executed
        if flag_column not in df.columns:
            return {
                'status': ValidationStatus.SKIPPED,
                'reason': 'validation_not_run'
            }
        
        # Statistics
        total_count = len(df)
        failed_count = int(df[flag_column].sum())
        
        if failed_count == 0:
            return {
                'status': ValidationStatus.PASSED,
                'total_rows': total_count,
                'failed_count': 0
            }
        
        # Failure report
        report = {
            'status': ValidationStatus.FAILED,
            'total_rows': total_count,
            'failed_count': failed_count,
            'failed_percentage': round(failed_count / total_count * 100, 2)
        }
        
        # Attach example rows
        if example_columns:
            examples = df[df[flag_column]][example_columns].head(5)
            report['examples'] = examples.to_dict('records') if not examples.empty else []
        
        return report
    