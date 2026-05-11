from datetime import datetime, timezone

from src.orion.config import CONFIG, JPL_HORIZONS_BRONZE_COLUMNS
from src.orion.ingestion.jpl_horizons import (
    align_bronze_dataframe,
    build_bronze_record,
    build_request_params,
    raw_jpl_horizons_table_name,
)


def test_build_request_params_uses_configured_mission_window():
    params = build_request_params()

    assert params["START_TIME"] == CONFIG.mission_start_date
    assert params["STOP_TIME"] == CONFIG.mission_end_date
    assert params["COMMAND"] == "301"
    assert params["CENTER"] == "500@399"


def test_build_bronze_record_contains_expected_columns():
    record = build_bronze_record(
        response_body='{"result": "ok"}',
        response_status_code=200,
        request_url="https://ssd.jpl.nasa.gov/api/horizons.api?format=json",
        ingestion_run_id="test-run",
        ingested_at=datetime(2026, 4, 1, 12, 30, tzinfo=timezone.utc),
    )

    assert tuple(record.keys()) == JPL_HORIZONS_BRONZE_COLUMNS
    assert record["ingestion_run_id"] == "test-run"
    assert record["source_system"] == "nasa_jpl_horizons"
    assert record["source_endpoint"] == "horizons_vectors"
    assert record["ingested_date"].isoformat() == "2026-04-01"
    assert record["mission_start_date"].isoformat() == CONFIG.mission_start_date
    assert record["mission_end_date"].isoformat() == CONFIG.mission_end_date


def test_raw_jpl_horizons_table_name():
    assert (
        raw_jpl_horizons_table_name()
        == "dbw_orion_dev_uks_001.orion_bronze.raw_jpl_horizons"
    )


def test_align_bronze_dataframe_selects_columns_in_contract_order(monkeypatch):
    selected_columns = []

    class FakeColumn:
        def __init__(self, name):
            self.name = name
            self.column_type = None

        def cast(self, column_type):
            self.column_type = column_type
            return self

        def alias(self, name):
            selected_columns.append((self.name, self.column_type, name))
            return self

    class FakeFunctions:
        @staticmethod
        def col(name):
            return FakeColumn(name)

    class FakeDataFrame:
        def select(self, *columns):
            return columns

    import src.orion.ingestion.jpl_horizons as jpl_horizons

    monkeypatch.setattr(jpl_horizons, "F", FakeFunctions, raising=False)

    # Patch the local import used inside align_bronze_dataframe without requiring PySpark locally.
    monkeypatch.setitem(__import__("sys").modules, "pyspark", type("FakePySpark", (), {})())
    monkeypatch.setitem(__import__("sys").modules, "pyspark.sql", type("FakeSql", (), {"functions": FakeFunctions})())

    align_bronze_dataframe(FakeDataFrame())

    assert tuple(column for column, _, _ in selected_columns) == JPL_HORIZONS_BRONZE_COLUMNS
    assert all(column == alias for column, _, alias in selected_columns)
