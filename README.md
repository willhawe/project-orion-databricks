# Project Orion

Project Orion is an Azure Databricks lakehouse project that ingests NASA Artemis II-era orbital ephemeris and space-weather data, transforms it through a medallion architecture, and serves curated Gold tables for Power BI.

The project demonstrates Databricks engineering patterns around Unity Catalog, Delta Lake, PySpark, notebook workflows, local development with Cursor, and governed lakehouse table design.

## Architecture

Project Orion uses the development Unity Catalog namespace:

```text
dbw_orion_dev_uks_001
```

Schemas:

```text
orion_bronze
orion_silver
orion_gold
orion_audit
orion_config
```

For this development build, the project uses the Databricks workspace catalog. In production, these schemas would typically move into a dedicated catalog such as `orion_prod`, backed by ADLS Gen2 managed storage.

## Data Sources

- NASA/JPL Horizons API: Moon geocentric ephemeris vectors for `2026-04-01` to `2026-04-10`.
- NASA DONKI API: space-weather event responses for CME, FLR, GST, IPS, and SEP endpoints.

## Implemented Tables

Bronze:

```text
dbw_orion_dev_uks_001.orion_bronze.raw_jpl_horizons
dbw_orion_dev_uks_001.orion_bronze.raw_donki_events
```

Silver:

```text
dbw_orion_dev_uks_001.orion_silver.jpl_ephemeris_vectors
dbw_orion_dev_uks_001.orion_silver.donki_space_weather_events
dbw_orion_dev_uks_001.orion_silver.telemetry_space_weather_hourly
```

Gold:

```text
dbw_orion_dev_uks_001.orion_gold.mission_timeline
dbw_orion_dev_uks_001.orion_gold.pbi_mission_timeline
dbw_orion_dev_uks_001.orion_gold.pbi_space_weather_events
dbw_orion_dev_uks_001.orion_gold.pbi_activity_summary
```

Audit:

```text
dbw_orion_dev_uks_001.orion_audit.workspace_smoke_test
dbw_orion_dev_uks_001.orion_audit.ingestion_runs
```

## Notebook Run Order

Run these through the Databricks extension with **Run File as Workflow** when using serverless compute:

```text
notebooks/00_setup/00_workspace_smoke_test.py.ipynb
notebooks/00_setup/01_create_bronze_tables.py.ipynb
notebooks/01_bronze_ingestion/01_ingest_jpl_horizons.py.ipynb
notebooks/01_bronze_ingestion/02_ingest_donki_events.py.ipynb
notebooks/02_silver_transformation/01_clean_ephemeris.py.ipynb
notebooks/02_silver_transformation/02_clean_space_weather.py.ipynb
notebooks/02_silver_transformation/03_join_telemetry_space_weather.py.ipynb
notebooks/03_gold_serving/01_build_mission_timeline.py.ipynb
notebooks/03_gold_serving/02_build_powerbi_tables.py.ipynb
```

## Repository Layout

```text
docs/                  Architecture and development notes
notebooks/             Databricks setup, Bronze, Silver, and Gold notebooks
src/orion/             Reusable Python config, ingestion, and transform modules
tests/                 Local unit tests for pure-Python logic
workflows/             Workflow definition placeholder
powerbi/               Power BI serving table notes
scratch/               Local Databricks connection checks
```

## Local Development

Create and activate a Python 3.11 virtual environment:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

Run local tests:

```bash
pytest -q
```

Databricks Connect is intentionally not pinned in `requirements-dev.txt`; use the Cursor Databricks extension Python Environment checklist to install/configure the compatible version.

See [docs/local-databricks-development.md](docs/local-databricks-development.md).

## Current Validation

The implemented pipeline has been run successfully through Databricks serverless workflow runs. Local pure-Python tests validate request construction, Bronze contracts, parsing logic, table helpers, and SQL generation.

Expected local test result at this stage:

```text
35 passed
```
