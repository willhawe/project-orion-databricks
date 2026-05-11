from src.orion.config import (
    CONFIG,
    JPL_HORIZONS_BRONZE_COLUMNS,
    JPL_HORIZONS_BRONZE_COLUMN_TYPES,
    audit_table_name,
    bronze_table_name,
    config_table_name,
    full_table_name,
    schema_name,
)


def test_catalog_is_set():
    assert CONFIG.catalog == "dbw_orion_dev_uks_001"


def test_mission_window_is_set():
    assert CONFIG.mission_start_date == "2026-04-01"
    assert CONFIG.mission_end_date == "2026-04-10"


def test_schema_name_returns_expected_schema():
    assert schema_name("bronze") == "orion_bronze"
    assert schema_name("silver") == "orion_silver"
    assert schema_name("gold") == "orion_gold"


def test_full_table_name():
    assert (
        full_table_name("orion_bronze", "raw_jpl_horizons")
        == "dbw_orion_dev_uks_001.orion_bronze.raw_jpl_horizons"
    )


def test_table_name_helpers():
    assert (
        bronze_table_name("raw_jpl_horizons")
        == "dbw_orion_dev_uks_001.orion_bronze.raw_jpl_horizons"
    )
    assert (
        audit_table_name("ingestion_runs")
        == "dbw_orion_dev_uks_001.orion_audit.ingestion_runs"
    )
    assert (
        config_table_name("pipeline_settings")
        == "dbw_orion_dev_uks_001.orion_config.pipeline_settings"
    )


def test_jpl_horizons_bronze_columns_match_expected_table_schema():
    assert JPL_HORIZONS_BRONZE_COLUMNS == (
        "ingestion_run_id",
        "source_system",
        "source_endpoint",
        "request_url",
        "request_params_json",
        "response_status_code",
        "response_body",
        "response_hash",
        "ingested_at",
        "ingested_date",
        "mission_name",
        "mission_start_date",
        "mission_end_date",
    )


def test_jpl_horizons_bronze_column_types_align_with_columns():
    assert tuple(column for column, _ in JPL_HORIZONS_BRONZE_COLUMN_TYPES) == JPL_HORIZONS_BRONZE_COLUMNS
    assert dict(JPL_HORIZONS_BRONZE_COLUMN_TYPES) == {
        "ingestion_run_id": "string",
        "source_system": "string",
        "source_endpoint": "string",
        "request_url": "string",
        "request_params_json": "string",
        "response_status_code": "int",
        "response_body": "string",
        "response_hash": "string",
        "ingested_at": "timestamp",
        "ingested_date": "date",
        "mission_name": "string",
        "mission_start_date": "date",
        "mission_end_date": "date",
    }
