from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator

# Ensure project root is importable when running in Airflow.
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.api_client import CoinGeckoAPIError, CoinGeckoClient
from src.constants import SchemaValidationStatus, ValidationFields
from src.validators import CryptoDataValidator

logger = logging.getLogger(__name__)

DATA_BASE_PATH = "/opt/airflow/data"

default_args = {
    "owner": "data_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def _get_data_path(subdir: str) -> Path:
    path = Path(DATA_BASE_PATH) / subdir
    path.mkdir(parents=True, exist_ok=True)
    return path


def fetch_coingecko_data(**context) -> int:
    """
    Fetch market data from CoinGecko, store raw CSV, and push file path via XCom.
    """
    
    client = CoinGeckoClient()

    try:
        data = client.get_markets_data()
    except CoinGeckoAPIError as e:
        logger.error(f"CoinGecko API error: {str(e)}")
        raise

    execution_date = context["ds_nodash"]
    raw_path = _get_data_path("raw")
    file_path = raw_path / f"crypto_raw_{execution_date}.csv"

    df = pd.DataFrame([record.model_dump() for record in data])
    df.to_csv(file_path, index=False, encoding="utf-8")

    context["ti"].xcom_push(key="record_count", value=len(df))
    context["ti"].xcom_push(key="raw_file_path", value=str(file_path))

    logger.info(f"Saved raw data: {file_path} (rows={len(df)})")
    return len(df)


def check_schema(**context) -> str:
    """
    Branch on schema validation result.
    """

    raw_file_path = context["ti"].xcom_pull(key="raw_file_path", task_ids="fetch_coingecko_data")
    if not raw_file_path or not Path(raw_file_path).exists():
        context["ti"].xcom_push(key="schema_result", value=SchemaValidationStatus.EMPTY_DATA)
        context["ti"].xcom_push(key="error_message", value=f"Raw file not found: {raw_file_path}")
        return "handle_schema_failure"

    try:
        df = pd.read_csv(raw_file_path)
    except pd.errors.EmptyDataError:
        error_msg = f"CSV file is completely empty: {raw_file_path}"
        logger.error(error_msg)
        context["ti"].xcom_push(key="schema_result", value=SchemaValidationStatus.EMPTY_DATA)
        context["ti"].xcom_push(key="error_message", value=error_msg)
        return "handle_schema_failure"
    except Exception as e:
        error_msg = f"Unexpected error reading CSV: {str(e)}"
        logger.error(error_msg)
        context["ti"].xcom_push(key="schema_result", value=SchemaValidationStatus.INVALID)
        context["ti"].xcom_push(key="error_message", value=error_msg)
        return "handle_schema_failure"
    
    validator = CryptoDataValidator()
    result = validator.validate_schema(df)

    context["ti"].xcom_push(key="schema_result", value=result)
    logger.info(f"Schema validation result: {result}")

    if result == SchemaValidationStatus.VALID:
        return "run_full_validation"
    return "handle_schema_failure"


def run_full_validation(**context) -> Dict[str, Any]:
    """
    Run all flag_* validations and generate a validation report.
    """

    raw_file_path = context["ti"].xcom_pull(key="raw_file_path", task_ids="fetch_coingecko_data")
    if not raw_file_path or not Path(raw_file_path).exists():
        raise FileNotFoundError(f"Raw file not found: {raw_file_path}")

    df = pd.read_csv(raw_file_path)
    validator = CryptoDataValidator()
    
    df = df.copy()
    df = validator.flag_invalid_numeric_types(df)
    df = validator.flag_abnormal_prices(df)
    df = validator.flag_invalid_market_cap(df)
    df = validator.flag_missing_values(df)
    df = validator.flag_duplicates(df)
    df = validator.add_metadata(df)

    execution_date = context["ds_nodash"]
    flagged_path = _get_data_path("flagged")
    flagged_file = flagged_path / f"flagged_{execution_date}.csv"

    df.to_csv(flagged_file, index=False, encoding="utf-8")

    validation_stats = {
        "total_rows": len(df),
        "invalid_types": int(df[ValidationFields.HAS_NON_NUMERIC_VALUE].sum()),
        "invalid_prices": int(df[ValidationFields.HAS_ABNORMAL_PRICE].sum()),
        "invalid_market_cap": int(df[ValidationFields.HAS_INVALID_MARKET_CAP].sum()),
        "missing_values": int(df[ValidationFields.HAS_MISSING_VALUES].sum()),
        "duplicates": int(df[ValidationFields.HAS_DUPLICATE].sum()),
    }

    context["ti"].xcom_push(key="flagged_data_path", value=str(flagged_file))
    context["ti"].xcom_push(key="validation_stats", value=validation_stats)

    logger.info(f"Saved flagged data: {flagged_file}")
    logger.info(f"Validation statistics: {validation_stats}")

    return validation_stats


def generate_quality_report(**context) -> None:
    """
    Generate and persist a validation report.
    """

    flagged_file_path = context["ti"].xcom_pull(key="flagged_data_path", task_ids="run_full_validation")
    if not flagged_file_path or not Path(flagged_file_path).exists():
        raise FileNotFoundError(f"File not found: {flagged_file_path}")

    df = pd.read_csv(flagged_file_path)
    validator = CryptoDataValidator()
    report = validator.generate_validation_report(df)
    
    execution_date = context['ds_nodash']
    reports_path = _get_data_path("reports")      
    report_file = reports_path / f"quality_report_{execution_date}.json"

    with open(report_file, "w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    context["ti"].xcom_push(key="report_file_path", value=str(report_file))
    context["ti"].xcom_push(key="quality_report", value=report)

    logger.info(f"Saved validation report: {report_file}")


def handle_schema_failure(**context) -> None:
    """
    Handle schema failure, only generate log.
    """

    schema_status = context["ti"].xcom_pull(key="schema_result", task_ids="check_schema")
    error_message = context["ti"].xcom_pull(key="error_message", task_ids="check_schema")

    logger.error(
        f"Schema validation failed\n"
        f"  Status: {schema_status}\n"
        f"  Error: {error_message}\n"
        f"  Execution date: {context['ds']}"
    )


def generate_schema_error_report(**context) -> None:
    """
    Generate and persist a schema error report.
    """

    schema_status_value = context["ti"].xcom_pull(key="schema_result", task_ids="check_schema")
    
    try:
        schema_status = SchemaValidationStatus(schema_status_value)
    except Exception:
        schema_status = SchemaValidationStatus.INVALID_TYPE

    validator = CryptoDataValidator()
    report = validator.generate_schema_error_report(schema_status)

    execution_date = context["ds_nodash"]
    reports_path = _get_data_path("reports")
    report_file = reports_path / f"schema_error_report_{execution_date}.json"

    with open(report_file, "w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    context["ti"].xcom_push(key="error_report_file_path", value=str(report_file))
    context["ti"].xcom_push(key="schema_error_report", value=report)

    logger.info(f"Saved schema error report: {report_file}")


def notify_validation_complete(**context) -> None:
    """
    Notification stub for validation complete.
    """

    validation_stats = context["ti"].xcom_pull(key="validation_stats", task_ids="run_full_validation")
    report = context["ti"].xcom_pull(key="quality_report", task_ids="generate_quality_report")
    report_file_path = context["ti"].xcom_pull(key="report_file_path", task_ids="generate_quality_report")

    summary = {
        "status": report.get("status", "unknown"),
        "execution_date": context["ds"],
        "report_file_path": report_file_path,
        "data_quality": validation_stats or {},
        "validation_summary": report.get("summary", {}),
    }

    if report.get("status") == "FAILED":
        logger.warning("Validation completed with quality issues: %s", 
                      json.dumps(summary, indent=2, ensure_ascii=False))
    else:
        logger.info("Validation completed successfully: %s", 
                   json.dumps(summary, indent=2, ensure_ascii=False))
    
    # TODO: integrate Slack/Email notification.


def notify_schema_failure(**context) -> None:
    """
    Notification stub for schema failures.
    """

    schema_status = context["ti"].xcom_pull(key="schema_result", task_ids="check_schema")
    error_report_file_path = context["ti"].xcom_pull(key="error_report_file_path", task_ids="handle_schema_failure")
    error_message = context["ti"].xcom_pull(key="error_message")

    summary = {
        "status": "failed",
        "execution_date": context["ds"],
         "error_report_file_path": error_report_file_path,
        "schema_status": getattr(schema_status, "value", str(schema_status)),
        "error_message": error_message,
    }

    logger.error("Schema failure summary: %s", json.dumps(summary, indent=2, ensure_ascii=False))
    # TODO: integrate Slack/Email notification.


with DAG(
    dag_id="crypto_market_data_pipeline",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 21),
    catchup=False,
    tags=["crypto", "validation"],
) as dag:
    fetch_task = PythonOperator(
        task_id="fetch_coingecko_data",
        python_callable=fetch_coingecko_data,
        retries=3,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=5),
    )

    check_schema_task = BranchPythonOperator(
        task_id="check_schema",
        python_callable=check_schema,
    )

    run_full_validation_task = PythonOperator(
        task_id="run_full_validation",
        python_callable=run_full_validation,
        # execution_timeout=timedelta(minutes=10)
    )

    generate_quality_report_task = PythonOperator(
        task_id='generate_quality_report',
        python_callable=generate_quality_report,
        retries=2,
        retry_delay=timedelta(seconds=30),
    )

    handle_schema_failure_task = PythonOperator(
        task_id="handle_schema_failure",
        python_callable=handle_schema_failure,
    )

    generate_error_report_task = PythonOperator(
        task_id="generate_error_report",
        python_callable=generate_schema_error_report,
        retries=2,
        retry_delay=timedelta(seconds=30),
    )

    notify_validation_complete_task = PythonOperator(
        task_id="notify_validation_complete",
        python_callable=notify_validation_complete,
        trigger_rule="all_success",
        retries=0,
    )

    notify_schema_failure_task = PythonOperator(
        task_id="notify_schema_failure",
        python_callable=notify_schema_failure,
        trigger_rule="all_success",
        retries=0,
    )

    end_task = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    fetch_task >> check_schema_task
    check_schema_task >> [run_full_validation_task, handle_schema_failure_task]
    run_full_validation_task >> generate_quality_report_task >> notify_validation_complete_task >> end_task
    handle_schema_failure_task >> generate_error_report_task >> notify_schema_failure_task >> end_task

