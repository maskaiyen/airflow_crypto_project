# Crypto Market Data Validation Pipeline

![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-3.x-017CEE?logo=apacheairflow)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)
![Pydantic](https://img.shields.io/badge/Pydantic-v2-E92063?logo=pydantic)
![pytest](https://img.shields.io/badge/Tested-pytest-0A9EDC?logo=pytest)

An end-to-end data pipeline built with Apache Airflow that fetches cryptocurrency market data from the CoinGecko API, validates data quality, and produces flagged datasets and structured reports — without modifying source data.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Apache Airflow 3.x (`BranchPythonOperator`, XCom, retries) |
| Data Validation | Pandas, Pydantic v2 |
| API Integration | CoinGecko REST API, Requests |
| Containerization | Docker Compose |
| Testing | pytest, unittest.mock |
| Language | Python 3.10+ |

---

## Pipeline Architecture

### DAG Flow

![DAG Graph View](images/dag-graph-view.png)
#### *Airflow Graph View showing the pipeline with branching logic*
#### *`fetch → schema check (VALID) → full validation → quality report → notify → end`*
#### *`fetch → schema check (INVALID) → error_log → schema_error_report → notify → end`*

---

### DAG Task Reference

| Step | Task ID | Description |
|------|---------|-------------|
| 1 | `fetch_coingecko_data` | Fetch 250 records from CoinGecko, save raw CSV, push path via XCom. Retries 3× with exponential backoff. |
| 2 | `check_schema` | Validate required fields and structure. **Branch**: VALID → full validation, INVALID → failure path. |
| 3a | `run_full_validation` | Run all `flag_*` validations, write flagged CSV. |
| 3b | `handle_schema_failure` | Log schema error status and message. |
| 4a | `generate_quality_report` | Write `quality_report_{date}.json` to `data/reports/`. |
| 4b | `generate_schema_error_report` | Write `schema_error_report_{date}.json` to `data/reports/`. |
| 5 | `notify_*` | Log summary. ⚠️ Slack/Email integration stub — ready to extend. |
| 6 | `end` | Join both branches (`none_failed_min_one_success`). |

---

## Project Structure

```
airflow_crypto_project/
├── dags/
│   └── crypto_market_data_pipeline.py   # Airflow DAG definition
├── src/
│   ├── api_client.py                    # CoinGecko HTTP client + error handling
│   ├── validators.py                    # Schema + flag validations + report generation
│   ├── schemas.py                       # Pydantic model for API response
│   ├── constants.py                     # Enums: SchemaValidationStatus, ValidationFields
│   └── report_types.py                  # TypedDicts for structured reports
├── tests/
│   ├── test_api_client.py
│   └── test_validators.py
├── data/
│   ├── raw/          # crypto_raw_YYYYMMDD.csv
│   ├── flagged/      # flagged_YYYYMMDD.csv  (original columns + 6 flag columns)
│   └── reports/      # quality_report_YYYYMMDD.json / schema_error_report_YYYYMMDD.json
├── docker-compose.yaml
├── requirements.txt
└── .env.example
```

---

## Quick Start

**1. Clone and configure**
```bash
git clone https://github.com/maskaiyen/airflow_crypto_project.git
cd airflow_crypto_project
cp .env.example .env
```

Fill in `.env` — generate required keys:
```bash
# AIRFLOW__CORE__FERNET_KEY
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# AIRFLOW__WEBSERVER__SECRET_KEY
openssl rand -hex 32
```

**2. Start Airflow**
```bash
docker compose up airflow-init
docker compose up -d
```

**3. Trigger the DAG**

Open http://localhost:8080 (user/pass: `airflow` / `airflow`), navigate to `crypto_market_data_pipeline`, and click **Trigger DAG**.

**4. Run tests (local)**
```bash
pytest tests/ -v
```

> For full environment variable reference, see [`.env.example`](.env.example).

---

## Execution Results

### Successful Run

![Successful DAG Run](images/successful-run.png)
*250 records fetched, validated, and flagged. Quality report persisted to `data/reports/`.*

### Schema Failure Handling

![Schema Failure Handling](images/schema-failure-handling.png)
*`BranchPythonOperator` correctly routes to the failure path when schema validation fails:*
*`fetch → schema check (INVALID) → handle_schema_failure → schema error report → notify → end`*

---

## Output Sample

Flagged CSV appends 6 columns to the original data — **source data is never modified**:

| Column | Type | Description |
|--------|------|-------------|
| `has_non_numeric_value` | bool | Non-numeric value found in numeric fields |
| `has_abnormal_price` | bool | `current_price` outside $0.000001–$1,000,000 |
| `has_invalid_market_cap` | bool | `market_cap` deviates >5% from `price × supply`, or ≤ 0 |
| `has_missing_values` | bool | Null in any required field |
| `has_duplicate` | bool | Duplicate `id` (first occurrence kept) |
| `validated_at` | str | ISO timestamp (Asia/Taipei) |

Quality report structure (`data/reports/quality_report_YYYYMMDD.json`):

```json
{
  "status": "failed",
  "stage": "data_validation",
  "total_rows": 250,
  "validations": {
    "price_range": {
      "status": "failed",
      "total_rows": 250,
      "failed_count": 3,
      "failed_percentage": 1.2,
      "examples": [{"symbol": "btt", "name": "BitTorrent", "current_price": 3.39633e-07}]
    },
    "numeric_types": {"status": "passed", "total_rows": 250, "failed_count": 0},
    "market_cap": {"status": "passed", "total_rows": 250, "failed_count": 0},
    "missing_values":{"status": "passed", "total_rows": 250, "failed_count": 0},
    "duplicates": {"status": "passed", "total_rows": 250, "failed_count": 0},
  },
  "summary": {
    "total": 5,
    "executed": 5,
    "passed": ["numeric_types", "market_cap", "missing_values", "duplicates"],
    "failed": ["price_range"],
    "skipped": []
  }
}
```